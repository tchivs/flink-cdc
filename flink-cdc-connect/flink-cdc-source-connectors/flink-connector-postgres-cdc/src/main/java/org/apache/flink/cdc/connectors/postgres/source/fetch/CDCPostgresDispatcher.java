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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.schema.SchemaDispatcher;
import org.apache.flink.cdc.connectors.postgres.source.utils.SchemaFingerprints;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/** Postgres Dispatcher for cdc source with watermark and partition routing. */
public class CDCPostgresDispatcher extends PostgresEventDispatcher<TableId>
        implements WatermarkDispatcher, SchemaDispatcher, PartitionRouteAware {

    private static final Logger LOG = LoggerFactory.getLogger(CDCPostgresDispatcher.class);

    @FunctionalInterface
    interface TableSchemaLoader {
        TableSchemaLoadResult load(TableId tableId) throws Exception;
    }

    static final class TableSchemaLoadResult {

        private static final TableSchemaLoadResult NOT_LOADED =
                new TableSchemaLoadResult(false, null);

        private final boolean loaded;
        @Nullable private final TableId parentTableId;

        private TableSchemaLoadResult(boolean loaded, @Nullable TableId parentTableId) {
            this.loaded = loaded;
            this.parentTableId = parentTableId;
        }

        static TableSchemaLoadResult notLoaded() {
            return NOT_LOADED;
        }

        static TableSchemaLoadResult loaded() {
            return new TableSchemaLoadResult(true, null);
        }

        static TableSchemaLoadResult loadedAsChildOf(TableId parentTableId) {
            return new TableSchemaLoadResult(true, parentTableId);
        }

        boolean isLoaded() {
            return loaded;
        }

        @Nullable
        TableId getParentTableId() {
            return parentTableId;
        }
    }

    private static final TableSchemaLoader NO_OP_SCHEMA_LOADER =
            tableId -> TableSchemaLoadResult.notLoaded();
    private static final BiConsumer<TableId, TableId> NO_OP_ROUTING_LISTENER =
            (childId, parentId) -> {};

    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private volatile Map<TableId, TableId> childToParentMapping;
    private volatile Set<TableId> partitionParents;
    private final DatabaseSchema<?> schemaRef;
    private final DataCollectionFilters.DataCollectionFilter dataCollectionFilter;
    private final TableSchemaLoader tableSchemaLoader;
    private final BiConsumer<TableId, TableId> partitionRouteListener;
    private final Set<TableId> tablesWithoutPartitionRoute = ConcurrentHashMap.newKeySet();

    /**
     * Tracks the last dispatched schema fingerprint per (routed) table to avoid emitting duplicate
     * SchemaChangeEvents when the schema has not actually changed. Uses {@link
     * SchemaFingerprints#computeSchemaFingerprint(Table)} for collision-resistant fingerprinting.
     */
    private final Map<TableId, Integer> lastDispatchedSchemaFingerprint = new ConcurrentHashMap<>();

    /** Tracks parent TableIds whose schema has already been registered (avoid repeated work). */
    private final Set<TableId> registeredParentSchemas = ConcurrentHashMap.newKeySet();

    public CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            DatabaseSchema schema,
            ChangeEventQueue queue,
            DataCollectionFilters.DataCollectionFilter filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster,
            Map<TableId, TableId> childToParentMapping) {
        this(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                heartbeatFactory,
                schemaNameAdjuster,
                childToParentMapping,
                NO_OP_SCHEMA_LOADER,
                NO_OP_ROUTING_LISTENER);
    }

    public CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            DatabaseSchema schema,
            ChangeEventQueue queue,
            DataCollectionFilters.DataCollectionFilter filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster,
            Map<TableId, TableId> childToParentMapping,
            TableSchemaLoader tableSchemaLoader) {
        this(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                heartbeatFactory,
                schemaNameAdjuster,
                childToParentMapping,
                tableSchemaLoader,
                NO_OP_ROUTING_LISTENER);
    }

    CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            DatabaseSchema schema,
            ChangeEventQueue queue,
            DataCollectionFilters.DataCollectionFilter filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster,
            Map<TableId, TableId> childToParentMapping,
            TableSchemaLoader tableSchemaLoader,
            BiConsumer<TableId, TableId> partitionRouteListener) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                heartbeatFactory,
                schemaNameAdjuster);
        this.topic = topicSelector.getPrimaryTopic();
        this.queue = queue;
        this.schemaRef = schema;
        this.dataCollectionFilter = filter;
        this.tableSchemaLoader =
                tableSchemaLoader == null ? NO_OP_SCHEMA_LOADER : tableSchemaLoader;
        this.partitionRouteListener =
                partitionRouteListener == null ? NO_OP_ROUTING_LISTENER : partitionRouteListener;
        updatePartitionRouting(childToParentMapping);
    }

    synchronized void updatePartitionRouting(Map<TableId, TableId> childToParentMapping) {
        Map<TableId, TableId> updatedMapping =
                childToParentMapping == null ? Collections.emptyMap() : childToParentMapping;
        this.childToParentMapping = Collections.unmodifiableMap(new HashMap<>(updatedMapping));
        this.partitionParents = Collections.unmodifiableSet(new HashSet<>(updatedMapping.values()));
        this.tablesWithoutPartitionRoute.clear();
    }

    @Override
    public void dispatchWatermarkEvent(
            Map<String, ?> sourcePartition,
            SourceSplitBase sourceSplit,
            Offset watermark,
            WatermarkKind watermarkKind)
            throws InterruptedException {
        SourceRecord sourceRecord =
                WatermarkEvent.create(
                        sourcePartition, topic, sourceSplit.splitId(), watermarkKind, watermark);
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    @Override
    public void dispatch(Table table) {
        Table routed = routeTable(table);
        TableId routedId = routed.id();
        int fingerprint = SchemaFingerprints.computeSchemaFingerprint(routed);
        Integer lastFingerprint = lastDispatchedSchemaFingerprint.get(routedId);
        if (lastFingerprint != null && lastFingerprint == fingerprint) {
            // Schema unchanged for this routed table — skip redundant event
            return;
        }
        lastDispatchedSchemaFingerprint.put(routedId, fingerprint);

        PostgresSchemaRecord schemaRecord = new PostgresSchemaRecord(routed);
        try {
            queue.enqueue(new DataChangeEvent(schemaRecord));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Dispatches a data change event. The tableId is expected to be already pre-routed by {@link
     * #preRouteTableId(TableId)} at the streaming event source level, so this method simply
     * delegates to the parent dispatcher.
     *
     * <p>Pre-routing at the source level (before emitter construction) is the single routing entry
     * point, ensuring both the dispatcher's dataCollectionId and the emitter's internal tableId are
     * aligned.
     */
    @Override
    public boolean dispatchDataChangeEvent(
            io.debezium.connector.postgresql.PostgresPartition partition,
            TableId dataCollectionId,
            ChangeRecordEmitter<io.debezium.connector.postgresql.PostgresPartition>
                    changeRecordEmitter)
            throws InterruptedException {
        return super.dispatchDataChangeEvent(partition, dataCollectionId, changeRecordEmitter);
    }

    /**
     * Ensures the parent table's schema is registered in the Debezium schema registry.
     *
     * <p>When {@code publish_via_partition_root=false} (PG10-compatible mode), PostgreSQL sends WAL
     * events tagged with child partition TableIds and only sends Relation messages for child
     * tables. The parent table never receives a Relation message, so its schema is never
     * registered. This method synthesizes the parent table schema from the child's schema on first
     * encounter, enabling Debezium's EventDispatcher to find the schema when processing routed
     * events.
     */
    private boolean ensureParentSchemaRegistered(TableId childId, TableId parentId) {
        if (registeredParentSchemas.contains(parentId)) {
            if (schemaRef instanceof RelationAwarePostgresSchema
                    && ((RelationAwarePostgresSchema) schemaRef).tableFor(parentId) != null) {
                return true;
            }
            registeredParentSchemas.remove(parentId);
            LOG.info(
                    "Partition parent schema '{}' was marked registered but is no longer present "
                            + "in the active Debezium schema registry; rebuilding it from child '{}'.",
                    parentId,
                    childId);
        }
        if (schemaRef instanceof RelationAwarePostgresSchema) {
            RelationAwarePostgresSchema pgSchema = (RelationAwarePostgresSchema) schemaRef;
            boolean registered = pgSchema.registerParentTableFromChild(childId, parentId);
            if (!registered && pgSchema.tableFor(childId) == null) {
                TableSchemaLoadResult loadResult =
                        loadSchema(
                                childId,
                                String.format(
                                        "refresh schema for partition child '%s' before routing it"
                                                + " to parent '%s'",
                                        childId, parentId));
                TableId discoveredParent = loadResult.getParentTableId();
                if (discoveredParent != null && !parentId.equals(discoveredParent)) {
                    LOG.warn(
                            "Partition child '{}' is currently routed to parent '{}', but JDBC "
                                    + "resolved parent '{}'. Keeping the existing routing for this "
                                    + "session.",
                            childId,
                            parentId,
                            discoveredParent);
                }
                if (loadResult.isLoaded()) {
                    registered = pgSchema.registerParentTableFromChild(childId, parentId);
                }
            }
            if (registered) {
                LOG.info(
                        "Registered parent table schema '{}' from child '{}' "
                                + "(publish_via_partition_root=false mode)",
                        parentId,
                        childId);
            }
            if (pgSchema.tableFor(parentId) != null) {
                registeredParentSchemas.add(parentId);
                return true;
            }
        }
        return false;
    }

    /**
     * Pre-routes a child partition tableId to its parent and ensures the parent schema is
     * registered. This is the single entry point used by the streaming event source (see {@link
     * PartitionRouteAware}) so that the emitter's internal {@code schema.tableFor(tableId)} call
     * uses the routed (parent) tableId and finds the registered schema.
     *
     * <p>This fixes the tableId misalignment between the emitter (which stores a fixed tableId at
     * construction time) and the dispatcher (which routes at dispatch time). Without pre-routing,
     * the emitter may call {@code schema.tableFor(childId)} on a schema that only has the parent
     * registered, causing a NullPointerException in PostgresChangeRecordEmitter.
     *
     * <p>When no routing is configured yet (childToParentMapping is empty), this method attempts a
     * lazy JDBC schema refresh so first-seen child partitions can still be routed before emitter
     * construction.
     */
    @Override
    public TableId preRouteTableId(TableId tableId) {
        if (tableId == null) {
            return null;
        }
        TableId routed = routeTableId(tableId);
        if (!routed.equals(tableId)) {
            if (!ensureParentSchemaRegistered(tableId, routed)) {
                throw new IllegalStateException(
                        String.format(
                                "Schema for routed partition parent '%s' is unavailable while "
                                        + "processing child partition '%s'. The child schema could "
                                        + "not be loaded before constructing the Debezium change "
                                        + "record emitter.",
                                routed, tableId));
            }
            return routed;
        }
        return tryResolveAndRouteMissingPartition(tableId);
    }

    private TableId routeTableId(TableId tableId) {
        if (tableId == null || childToParentMapping.isEmpty()) {
            return tableId;
        }
        return childToParentMapping.getOrDefault(tableId, tableId);
    }

    private TableId tryResolveAndRouteMissingPartition(TableId tableId) {
        if (tableId == null || !(schemaRef instanceof RelationAwarePostgresSchema)) {
            return tableId;
        }
        if (!((RelationAwarePostgresSchema) schemaRef).isPartitionRoutingEnabled()) {
            return tableId;
        }

        if (isKnownPartitionParent(tableId) || tablesWithoutPartitionRoute.contains(tableId)) {
            return tableId;
        }

        TableSchemaLoadResult loadResult =
                loadSchema(
                        tableId,
                        String.format(
                                "refresh schema and resolve partition parent for unknown table '%s'",
                                tableId));
        TableId parentId = loadResult.getParentTableId();
        if (parentId == null || tableId.equals(parentId)) {
            tablesWithoutPartitionRoute.add(tableId);
            return tableId;
        }
        if (!isKnownPartitionParent(parentId) && !dataCollectionFilter.isIncluded(parentId)) {
            LOG.debug(
                    "Table '{}' is a child of '{}', but that parent is not part of the current "
                            + "partition routing state. Leaving the table id unchanged.",
                    tableId,
                    parentId);
            tablesWithoutPartitionRoute.add(tableId);
            return tableId;
        }

        addPartitionRoute(tableId, parentId);
        if (!ensureParentSchemaRegistered(tableId, parentId)) {
            throw new IllegalStateException(
                    String.format(
                            "Schema for lazily discovered partition parent '%s' is unavailable "
                                    + "while processing child partition '%s'.",
                            parentId, tableId));
        }
        LOG.info(
                "Lazily discovered partition child '{}' -> parent '{}' before emitter "
                        + "construction.",
                tableId,
                parentId);
        return parentId;
    }

    private TableSchemaLoadResult loadSchema(TableId tableId, String action) {
        try {
            return tableSchemaLoader.load(tableId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to " + action, e);
        }
    }

    private boolean isKnownPartitionParent(TableId parentId) {
        return partitionParents.contains(parentId);
    }

    private void addPartitionRoute(TableId childId, TableId parentId) {
        boolean added = false;
        synchronized (this) {
            TableId existingParent = childToParentMapping.get(childId);
            if (parentId.equals(existingParent)) {
                return;
            }
            if (existingParent != null && !parentId.equals(existingParent)) {
                LOG.warn(
                        "Partition child '{}' was already routed to '{}', ignoring newly resolved "
                                + "parent '{}'.",
                        childId,
                        existingParent,
                        parentId);
                return;
            }
            Map<TableId, TableId> updated = new HashMap<>(childToParentMapping);
            updated.put(childId, parentId);
            childToParentMapping = Collections.unmodifiableMap(updated);
            Set<TableId> updatedParents = new HashSet<>(partitionParents);
            updatedParents.add(parentId);
            partitionParents = Collections.unmodifiableSet(updatedParents);
            added = true;
        }
        if (added) {
            partitionRouteListener.accept(childId, parentId);
        }
    }

    private Table routeTable(Table table) {
        if (table == null) {
            return null;
        }
        if (childToParentMapping.isEmpty()) {
            return table;
        }
        TableId parentTableId = routeTableId(table.id());
        if (parentTableId == null || parentTableId.equals(table.id())) {
            return table;
        }
        return table.edit().tableId(parentTableId).create();
    }
}
