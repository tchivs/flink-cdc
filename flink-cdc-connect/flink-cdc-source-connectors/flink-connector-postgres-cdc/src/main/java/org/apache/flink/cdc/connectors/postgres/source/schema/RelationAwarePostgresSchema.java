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

import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionGuard;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionTopology;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extends PostgresSchema to dispatch Relation messages as schema change events via the event queue
 * and expose buildAndRegisterSchema as public.
 */
public class RelationAwarePostgresSchema extends PostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(RelationAwarePostgresSchema.class);

    private SchemaDispatcher dispatcher;
    private PartitionGuard partitionGuard;

    /**
     * Routed partitions found detached while streaming. They are filtered from the relation that
     * revealed the DETACH on, so their earlier changes still reach the root and the later ones are
     * dropped, without a restart.
     */
    private final Set<TableId> detachedPartitions = new HashSet<>();

    private PartitionTopology partitionTopology;
    private int markerRelationId;
    private Table markerTable;

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

    public void setPartitionGuard(PartitionGuard partitionGuard) {
        this.partitionGuard = partitionGuard;
    }

    /** Follows partition DDL by stream position, the guard is not used then. */
    public void setPartitionTopology(PartitionTopology partitionTopology) {
        this.partitionTopology = partitionTopology;
        LOG.info(
                "Partition routing at the start of the stream: {}",
                partitionTopology.routing().routedPartitions());
        dispatcher.dispatchRouting(partitionTopology.routing());
    }

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        if (partitionTopology != null && PartitionTopology.isMarkerTable(table.id())) {
            // Filtered by Debezium, but its rows have to be decoded to reach the topology.
            markerRelationId = relationId;
            markerTable = table;
            return;
        }
        super.applySchemaChangesForTable(relationId, table);
        if (isFilteredOut(table.id())) {
            // Debezium sees the relation of every published table, filtered ones included.
            if (partitionTopology != null) {
                partitionTopology.checkFiltered(table.id());
            } else if (partitionGuard != null) {
                partitionGuard.checkFiltered(table.id());
            }
            return;
        }
        if (partitionTopology != null && partitionTopology.isUnrouted(table.id())) {
            return;
        }
        // pgoutput resends the relation of a partition after DETACH, before its next change.
        if (partitionGuard != null && partitionGuard.isDetached(table.id())) {
            LOG.warn(
                    "Partition {} was detached from its partition root, its changes from now on "
                            + "are no longer captured.",
                    table.id());
            detachedPartitions.add(table.id());
            return;
        }
        if (dispatcher != null) {
            dispatcher.dispatch(table);
        }
    }

    /**
     * Applies a row of the partition marker table read from the stream.
     *
     * @return whether the message was a marker row, which must not be dispatched
     */
    public boolean applyPartitionMarker(
            TableId tableId, ReplicationMessage message, PgConnectionSupplier connection)
            throws SQLException {
        if (partitionTopology == null || !PartitionTopology.isMarkerTable(tableId)) {
            return false;
        }
        if (message.getOperation() != ReplicationMessage.Operation.INSERT) {
            return true;
        }
        Long lsn = null;
        String parent = null;
        List<String> children = null;
        for (ReplicationMessage.Column column : message.getNewTupleList()) {
            Object value = column.getValue(connection, false);
            switch (column.getName()) {
                case "lsn":
                    lsn = ((Number) value).longValue();
                    break;
                case "parent":
                    parent = (String) value;
                    break;
                case "children":
                    children =
                            value == null
                                    ? null
                                    : Arrays.asList((String[]) ((Array) value).getArray());
                    break;
                default:
                    break;
            }
        }
        if (lsn == null || parent == null) {
            throw new IllegalStateException("Malformed partition marker row: " + message);
        }
        PartitionTopology.Marker marker = new PartitionTopology.Marker(lsn, parent, children);
        if (partitionTopology.apply(marker)) {
            LOG.info(
                    "Partition routing changed at {}: {}",
                    marker,
                    partitionTopology.routing().routedPartitions());
            dispatcher.dispatchRouting(partitionTopology.routing());
        }
        return true;
    }

    @Override
    protected boolean isFilteredOut(TableId id) {
        return detachedPartitions.contains(id) || super.isFilteredOut(id);
    }

    /** Changes of a detached partition are no-ops. */
    @Override
    public Table tableFor(TableId id) {
        return detachedPartitions.contains(id) ? null : super.tableFor(id);
    }

    /**
     * The decoder resolves every change through this: changes of a partition not routed at this
     * position are no-ops. Only the decoder lookup is filtered, the schema of such a partition is
     * still registered, so that its changes are captured as soon as it is routed.
     */
    @Override
    public Table tableFor(int relationId) {
        if (markerTable != null && relationId == markerRelationId) {
            return markerTable;
        }
        Table table = super.tableFor(relationId);
        if (table != null
                && partitionTopology != null
                && partitionTopology.isUnrouted(table.id())) {
            return null;
        }
        return table;
    }

    @Override
    public void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }
}
