/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source.schema;

import org.apache.flink.cdc.connectors.postgres.source.utils.SchemaFingerprints;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extends PostgresSchema to dispatch Relation messages as schema change events via the event queue
 * and expose buildAndRegisterSchema as public.
 *
 * <p>Also supports a {@link NewPartitionListener} that is notified when a pgoutput Relation ('R')
 * message arrives, including filtered runtime child relations. This allows partition detection to
 * be driven by WAL events rather than by a polling thread, while schema events are still dispatched
 * only for non-filtered tables.
 */
public class RelationAwarePostgresSchema extends PostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(RelationAwarePostgresSchema.class);

    private final Map<Integer, TableId> filteredRelationIdToTableId = new HashMap<>();

    /** Fast O(1) lookup set for filtered child partition TableIds. */
    private final Set<TableId> filteredTableIds = new HashSet<>();

    /**
     * Tracks schema fingerprints of child partitions to skip redundant overwriteTable calls. Key =
     * TableId, Value = schema fingerprint computed by {@link SchemaFingerprints}. Since all
     * children of a partition parent share the same schema, this avoids creating duplicate Table
     * objects for each Relation message.
     */
    private final Map<TableId, Integer> childSchemaFingerprints = new HashMap<>();

    /**
     * TOAST column lists for synthesized parent tables (publish_via_partition_root=false mode). The
     * base {@link PostgresSchema#getToastableColumnsForTableId(TableId)} only knows about tables
     * refreshed via JDBC, so synthesized logical parent tables would otherwise return an empty list
     * and unchanged-TOAST placeholders in UPDATE events would be mishandled. We mirror the child's
     * TOAST column list onto the parent at synthesis time.
     */
    private final Map<TableId, List<String>> syntheticToastableColumns = new HashMap<>();

    private final PostgresConnectorConfig config;

    /**
     * Whether partition routing is active for this schema instance. When false, {@link
     * #applySchemaChangesForTable} takes a fast path that skips partition-specific logic (filtered
     * child handling and schema fingerprinting) to avoid overhead for non-partition users.
     */
    private volatile boolean partitionRoutingEnabled;

    /**
     * Listener invoked when a pgoutput Relation message is received for a table, including filtered
     * runtime child relations.
     *
     * <p>This is used to detect previously unknown child partitions arriving in the WAL stream,
     * which triggers a streaming session restart to rebuild routing mappings. Implementations run
     * synchronously on the Debezium WAL processing thread, so they must stay lightweight and must
     * not perform JDBC or other blocking catalog work.
     */
    @FunctionalInterface
    public interface NewPartitionListener {
        /**
         * Called when a Relation message for the given table is applied to the schema.
         *
         * @param tableId the table whose Relation message was just processed
         */
        void onRelationSeen(TableId tableId);
    }

    private SchemaDispatcher dispatcher;
    private volatile NewPartitionListener partitionListener;

    public RelationAwarePostgresSchema(
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            PostgresDefaultValueConverter defaultValueConverter,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter) {
        super(config, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
        this.config = config;
    }

    public void setDispatcher(SchemaDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Sets the partition listener. The listener is called within the WAL processing thread whenever
     * a Relation message arrives, including for filtered runtime child relations, so it must remain
     * lightweight and JDBC-free. Setting to {@code null} removes the listener.
     */
    public void setPartitionListener(NewPartitionListener listener) {
        this.partitionListener = listener;
    }

    /**
     * Enables or disables partition routing mode. When disabled (the default), Relation messages
     * are processed through the base {@link PostgresSchema} path only, skipping partition-specific
     * overhead such as fingerprint computation.
     */
    public void setPartitionRoutingEnabled(boolean enabled) {
        this.partitionRoutingEnabled = enabled;
    }

    public boolean isPartitionRoutingEnabled() {
        return partitionRoutingEnabled;
    }

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        if (!partitionRoutingEnabled) {
            // Fast path for non-partition users: delegate to base class without partition-specific
            // overhead (fingerprint computation and filtered child handling).
            super.applySchemaChangesForTable(relationId, table);
            if (!isFilteredOut(table.id()) && dispatcher != null) {
                dispatcher.dispatch(table);
            }
            return;
        }

        boolean filtered = isFilteredOut(table.id());
        if (filtered) {
            // Child partitions are filtered out by the configured table filter, but we still
            // need their schema registered for WAL decoding. Store in our secondary index.
            filteredRelationIdToTableId.put(relationId, table.id());
            filteredTableIds.add(table.id());
            int fingerprint = SchemaFingerprints.computeSchemaFingerprint(table);
            Integer existing = childSchemaFingerprints.get(table.id());
            if (existing == null || existing != fingerprint) {
                // First time or schema changed — register the full table object
                tables().overwriteTable(table);
                childSchemaFingerprints.put(table.id(), fingerprint);
            }
            // Otherwise skip: schema is unchanged, no need to rebuild Table object
        } else {
            super.applySchemaChangesForTable(relationId, table);
        }
        if (!isFilteredOut(table.id()) && dispatcher != null) {
            dispatcher.dispatch(table);
        }
        NewPartitionListener listener = this.partitionListener;
        if (listener != null) {
            listener.onRelationSeen(table.id());
        }
    }

    @Override
    public Table tableFor(int relationId) {
        Table table = super.tableFor(relationId);
        if (table != null) {
            return table;
        }
        // Fallback to filtered relation index (child partitions)
        TableId filteredTableId = filteredRelationIdToTableId.get(relationId);
        return filteredTableId == null ? null : tables().forTable(filteredTableId);
    }

    @Override
    public Table tableFor(TableId id) {
        Table table = super.tableFor(id);
        if (table != null) {
            return table;
        }
        // O(1) lookup via dedicated Set instead of O(N) containsValue scan
        return filteredTableIds.contains(id) ? tables().forTable(id) : null;
    }

    @Override
    public void buildAndRegisterSchema(Table table) {
        // Debezium's RelationalDatabaseSchema.buildAndRegisterSchema only builds a TableSchema
        // and stores it in schemasByTableId — it does NOT update the Tables metadata store.
        // The standard Debezium path (refresh → refreshSchema) calls tables().overwriteTable
        // separately, but our partition-routing callers (registerRuntimeTable for non-filtered
        // tables, synthesizeParentTable) invoke buildAndRegisterSchema directly without a prior
        // overwriteTable. Centralising both operations here ensures all paths register the table
        // metadata and the TableSchema atomically.
        tables().overwriteTable(table);
        super.buildAndRegisterSchema(table);
    }

    /**
     * Refreshes one table from pg_catalog and registers it even when it is filtered out by the
     * configured capture filter.
     *
     * <p>This is used as a lazy fallback for logical decoding plugins such as decoderbufs that do
     * not send pgoutput Relation messages. It must not be called from {@link NewPartitionListener};
     * callers should invoke it only from code paths where a short blocking catalog lookup is
     * acceptable.
     */
    public synchronized boolean refreshTableFromJdbc(PostgresConnection connection, TableId tableId)
            throws SQLException {
        Table existing = tableFor(tableId);
        if (existing != null) {
            return true;
        }

        Tables refreshedTables = new Tables();
        connection.readSchema(
                refreshedTables,
                config.databaseName(),
                null,
                candidate ->
                        config.getTableFilters().dataCollectionFilter().isIncluded(candidate)
                                || tableId.equals(candidate),
                null,
                false);

        Table refreshed = refreshedTables.forTable(tableId);
        if (refreshed == null) {
            return false;
        }
        registerRuntimeTable(refreshed);
        return true;
    }

    private void registerRuntimeTable(Table table) {
        if (isFilteredOut(table.id())) {
            filteredTableIds.add(table.id());
            int fingerprint = SchemaFingerprints.computeSchemaFingerprint(table);
            Integer existing = childSchemaFingerprints.get(table.id());
            if (existing == null || existing != fingerprint) {
                tables().overwriteTable(table);
                childSchemaFingerprints.put(table.id(), fingerprint);
            }
            return;
        }
        buildAndRegisterSchema(table);
    }

    /**
     * Registers a parent table schema synthesized from a child partition's schema.
     *
     * <p>When {@code publish_via_partition_root=false}, PostgreSQL never sends a Relation message
     * for the parent table. This method creates the parent table entry in the schema registry using
     * the child's column structure (including its primary key column list, since PG10 partition
     * parents cannot define a PK themselves), enabling Debezium's EventDispatcher to find the
     * schema when processing routed (child→parent) data change events.
     *
     * <p>Behavior:
     *
     * <ul>
     *   <li>If the parent is already registered, validates PK consistency between the new child and
     *       the previously synthesized parent and emits a WARN if they differ. Does not overwrite
     *       the existing parent.
     *   <li>If the child has no primary key, logs an INFO note: UPDATE/DELETE events will have null
     *       record keys unless REPLICA IDENTITY FULL is configured.
     *   <li>Mirrors the child's TOAST column list onto the parent so that unchanged-TOAST
     *       placeholders in routed UPDATE events are handled correctly.
     * </ul>
     *
     * @param childId the child table whose schema to use as template
     * @param parentId the parent table to register
     * @return true if the parent was registered, false if already registered or child not found
     */
    public boolean registerParentTableFromChild(TableId childId, TableId parentId) {
        Table childTable = tableFor(childId);
        if (childTable == null) {
            return false; // Child schema not available yet
        }

        Table existingParent = tableFor(parentId);
        if (existingParent != null) {
            // Already registered — only validate PK consistency across siblings.
            List<String> existingPk = existingParent.primaryKeyColumnNames();
            List<String> childPk = childTable.primaryKeyColumnNames();
            if (!existingPk.equals(childPk)) {
                LOG.warn(
                        "Partition '{}' PK {} differs from previously synthesized parent '{}' PK {}. "
                                + "Key encoding will continue to use the first child's PK definition; "
                                + "this is unsafe if downstream relies on stable record keys.",
                        childId,
                        childPk,
                        parentId,
                        existingPk);
            }
            // Always keep TOAST list in sync — extend with any newly seen TOAST columns.
            mergeToastableColumns(parentId, childId);
            return false;
        }

        if (childTable.primaryKeyColumnNames().isEmpty()) {
            LOG.info(
                    "Synthesizing parent '{}' from child '{}' without primary key. "
                            + "UPDATE/DELETE events will have null record keys unless "
                            + "REPLICA IDENTITY FULL is configured on every child partition.",
                    parentId,
                    childId);
        }

        Table parentTable = childTable.edit().tableId(parentId).create();
        buildAndRegisterSchema(parentTable);
        mergeToastableColumns(parentId, childId);
        return true;
    }

    private void mergeToastableColumns(TableId parentId, TableId childId) {
        List<String> childToastable = super.getToastableColumnsForTableId(childId);
        if (childToastable.isEmpty()) {
            return;
        }
        List<String> existing = syntheticToastableColumns.get(parentId);
        if (existing == null) {
            syntheticToastableColumns.put(
                    parentId, Collections.unmodifiableList(new ArrayList<>(childToastable)));
            return;
        }
        // Union the column sets so that any TOAST column from any sibling is honored.
        LinkedHashSet<String> union = new LinkedHashSet<>(existing);
        boolean changed = union.addAll(childToastable);
        if (changed) {
            syntheticToastableColumns.put(
                    parentId, Collections.unmodifiableList(new ArrayList<>(union)));
        }
    }

    @Override
    public List<String> getToastableColumnsForTableId(TableId tableId) {
        List<String> base = super.getToastableColumnsForTableId(tableId);
        if (!base.isEmpty()) {
            return base;
        }
        List<String> synthesized = syntheticToastableColumns.get(tableId);
        return synthesized == null ? base : synthesized;
    }
}
