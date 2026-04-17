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

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

import java.util.HashMap;
import java.util.Map;

/**
 * Extends PostgresSchema to dispatch Relation messages as schema change events via the event queue
 * and expose buildAndRegisterSchema as public.
 *
 * <p>Also supports a {@link Pg10NewPartitionListener} that is notified when a pgoutput Relation
 * ('R') message arrives, including filtered runtime child relations. This allows PG10 partition
 * detection to be driven by WAL events rather than by a polling thread, while schema events are
 * still dispatched only for non-filtered tables.
 */
public class RelationAwarePostgresSchema extends PostgresSchema {

    private final Map<Integer, TableId> filteredRelationIdToTableId = new HashMap<>();

    /**
     * Listener invoked when a pgoutput Relation message is received for a table, including filtered
     * runtime child relations.
     *
     * <p>In PG10 mode this is used to detect previously unknown child partitions arriving in the
     * WAL stream, which triggers a streaming session restart to rebuild routing mappings.
     * Implementations run synchronously on the Debezium WAL processing thread, so they must stay
     * lightweight and must not perform JDBC or other blocking catalog work.
     */
    @FunctionalInterface
    public interface Pg10NewPartitionListener {
        /**
         * Called when a Relation message for the given table is applied to the schema.
         *
         * @param tableId the table whose Relation message was just processed
         */
        void onRelationSeen(TableId tableId);
    }

    private SchemaDispatcher dispatcher;
    private volatile Pg10NewPartitionListener partitionListener;

    public RelationAwarePostgresSchema(
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            PostgresDefaultValueConverter defaultValueConverter,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter) {
        super(config, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
    }

    public void setDispatcher(SchemaDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Sets the PG10 partition listener. The listener is called within the WAL processing thread
     * whenever a Relation message arrives, including for filtered runtime child relations, so it
     * must remain lightweight and JDBC-free. Setting to {@code null} removes the listener.
     */
    public void setPartitionListener(Pg10NewPartitionListener listener) {
        this.partitionListener = listener;
    }

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        if (isFilteredOut(table.id())) {
            filteredRelationIdToTableId.put(relationId, table.id());
            tables().overwriteTable(table);
        } else {
            super.applySchemaChangesForTable(relationId, table);
        }
        if (!isFilteredOut(table.id()) && dispatcher != null) {
            dispatcher.dispatch(table);
        }
        Pg10NewPartitionListener listener = this.partitionListener;
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
        TableId filteredTableId = filteredRelationIdToTableId.get(relationId);
        return filteredTableId == null ? null : tables().forTable(filteredTableId);
    }

    @Override
    public Table tableFor(TableId id) {
        Table table = super.tableFor(id);
        if (table != null) {
            return table;
        }
        return filteredRelationIdToTableId.containsValue(id) ? tables().forTable(id) : null;
    }

    @Override
    public void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }
}
