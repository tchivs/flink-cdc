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

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
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

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        super.applySchemaChangesForTable(relationId, table);
        if (isFilteredOut(table.id())) {
            // Debezium sees the relation of every published table, filtered ones included.
            if (partitionGuard != null) {
                partitionGuard.checkFiltered(table.id());
            }
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

    @Override
    protected boolean isFilteredOut(TableId id) {
        return detachedPartitions.contains(id) || super.isFilteredOut(id);
    }

    /**
     * The decoder resolves every change through this: changes of a detached partition are no-ops.
     */
    @Override
    public Table tableFor(TableId id) {
        return detachedPartitions.contains(id) ? null : super.tableFor(id);
    }

    @Override
    public void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }
}
