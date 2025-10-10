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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A CustomPostgresSchema similar to PostgresSchema with customization. */
public class CustomPostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPostgresSchema.class);

    // cache the schema for each table
    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>();
    private final PostgresConnection jdbcConnection;
    private final PostgresConnectorConfig dbzConfig;
    private PostgresSchema postgresSchema;

    @Nullable private final PostgresPartitionRouter partitionRouter;

    public CustomPostgresSchema(
            PostgresConnection jdbcConnection,
            PostgresSourceConfig sourceConfig,
            @Nullable PostgresPartitionRouter partitionRouter) {
        this.jdbcConnection = jdbcConnection;
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
        this.partitionRouter = partitionRouter;
        initializePostgresSchema();
    }

    private void initializePostgresSchema() {
        try {
            TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);
            PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                    PostgresObjectUtils.newPostgresValueConverterBuilder(dbzConfig);

            // Always use PostgresPartitionRoutingSchema since it handles both routing and
            // non-routing cases internally based on route.partition.events.to.parent config
            this.postgresSchema =
                    new PostgresPartitionRoutingSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));

            // If partition routing is enabled and a router is provided, populate it with
            // database-derived partition mappings for more accurate routing
            if (partitionRouter != null) {
                PostgresPartitionRoutingSchema routingSchema =
                        (PostgresPartitionRoutingSchema) this.postgresSchema;
                Map<TableId, TableId> childToParentMapping =
                        routingSchema.getChildToParentMapping();

                if (!childToParentMapping.isEmpty()) {
                    partitionRouter.preloadPartitionMappingFromDatabase(childToParentMapping);
                    LOG.info(
                            "Injected {} database-derived partition mappings into PostgresPartitionRouter",
                            childToParentMapping.size());
                }
            }
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to initialize PostgresSchema", e);
        }
    }

    public TableChange getTableSchema(TableId tableId) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                readTableSchema(Collections.singletonList(tableId));
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    public Map<TableId, TableChange> getTableSchema(List<TableId> tableIds) {
        // read schema from cache first
        Map<TableId, TableChange> tableChanges = new HashMap<>();

        List<TableId> unMatchTableIds = new ArrayList<>();
        for (TableId tableId : tableIds) {
            if (schemasByTableId.containsKey(tableId)) {
                tableChanges.put(tableId, schemasByTableId.get(tableId));
            } else {
                unMatchTableIds.add(tableId);
            }
        }

        if (!unMatchTableIds.isEmpty()) {
            try {
                readTableSchema(tableIds);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
            for (TableId tableId : unMatchTableIds) {
                if (schemasByTableId.containsKey(tableId)) {
                    tableChanges.put(tableId, schemasByTableId.get(tableId));
                } else {
                    throw new FlinkRuntimeException(
                            String.format("Failed to read table schema of table %s", tableId));
                }
            }
        }
        return tableChanges;
    }

    private List<TableChange> readTableSchema(List<TableId> tableIds) throws SQLException {
        List<TableChange> tableChanges = new ArrayList<>();

        final PostgresOffsetContext offsetContext =
                PostgresOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());

        for (TableId tableId : tableIds) {
            // Use postgresSchema.tableFor() which handles partition routing
            Table table = postgresSchema.tableFor(tableId);
            if (table == null) {
                throw new FlinkRuntimeException(
                        String.format("Failed to read table schema for table %s", tableId));
            }

            // set the events to populate proper sourceInfo into offsetContext
            offsetContext.event(tableId, Instant.now());

            // TODO: check whether we always set isFromSnapshot = true
            SchemaChangeEvent schemaChangeEvent =
                    SchemaChangeEvent.ofCreate(
                            partition,
                            offsetContext,
                            dbzConfig.databaseName(),
                            tableId.schema(),
                            null,
                            table,
                            true);

            for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                this.schemasByTableId.put(tableId, tableChange);
            }
            tableChanges.add(this.schemasByTableId.get(tableId));
        }
        return tableChanges;
    }
}
