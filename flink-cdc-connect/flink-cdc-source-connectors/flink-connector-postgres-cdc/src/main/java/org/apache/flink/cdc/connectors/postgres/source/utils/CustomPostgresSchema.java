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
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A CustomPostgresSchema similar to PostgresSchema with multi-table caching optimization.
 *
 * <p>This implementation leverages PostgreSQL API's ability to return all matching table schemas in
 * a single call, significantly reducing the number of database round trips in multi-table
 * scenarios.
 */
public class CustomPostgresSchema {

    // Thread-safe cache for table schemas
    private final Map<TableId, TableChange> schemasByTableId = new ConcurrentHashMap<>();
    private final PostgresConnection jdbcConnection;
    private final PostgresConnectorConfig dbzConfig;

    // Flag to mark whether all schemas have been loaded to avoid redundant full loading
    private volatile boolean allSchemasLoaded = false;

    public CustomPostgresSchema(
            PostgresConnection jdbcConnection, PostgresSourceConfig sourceConfig) {
        this.jdbcConnection = jdbcConnection;
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
    }

    public TableChange getTableSchema(TableId tableId) {
        // First, try to read schema from cache
        TableChange cachedSchema = schemasByTableId.get(tableId);
        if (cachedSchema != null) {
            return cachedSchema;
        }

        // If not in cache and haven't loaded all schemas yet, load all table schemas at once
        if (!allSchemasLoaded) {
            try {
                loadAllTableSchemas();
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to load all table schemas", e);
            }
        }

        // Check cache again after loading
        TableChange tableChange = schemasByTableId.get(tableId);
        if (tableChange == null) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read table schema of table %s", tableId));
        }

        return tableChange;
    }

    public Map<TableId, TableChange> getTableSchema(List<TableId> tableIds) {
        // Handle null or empty input
        if (tableIds == null || tableIds.isEmpty()) {
            return new HashMap<>();
        }

        Map<TableId, TableChange> tableChanges = new HashMap<>();
        List<TableId> missingTableIds = new ArrayList<>();

        // First, get existing schemas from cache
        for (TableId tableId : tableIds) {
            TableChange tableChange = schemasByTableId.get(tableId);
            if (tableChange != null) {
                tableChanges.put(tableId, tableChange);
            } else {
                missingTableIds.add(tableId);
            }
        }

        // If there are missing table schemas and we haven't loaded all schemas yet, load all
        if (!missingTableIds.isEmpty() && !allSchemasLoaded) {
            try {
                loadAllTableSchemas();
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to load all table schemas", e);
            }

            // Retry getting the missing table schemas
            for (TableId tableId : missingTableIds) {
                TableChange tableChange = schemasByTableId.get(tableId);
                if (tableChange != null) {
                    tableChanges.put(tableId, tableChange);
                } else {
                    throw new FlinkRuntimeException(
                            String.format("Failed to read table schema of table %s", tableId));
                }
            }
        }

        return tableChanges;
    }

    /**
     * Load all table schemas at once, leveraging PostgreSQL API's ability to return all matching
     * table schemas in a single call.
     *
     * <p>This method significantly reduces database round trips in multi-table scenarios by caching
     * all available schemas instead of loading them one by one.
     */
    private synchronized void loadAllTableSchemas() throws SQLException {
        // Double-checked locking to avoid redundant loading
        if (allSchemasLoaded) {
            return;
        }

        final PostgresOffsetContext offsetContext =
                PostgresOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());

        Tables tables = new Tables();
        try {
            // This call returns schema information for all matching tables
            jdbcConnection.readSchema(
                    tables,
                    dbzConfig.databaseName(),
                    null,
                    dbzConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read schema", e);
        }

        // Iterate through all returned table IDs and cache their schema information
        tables.tableIds()
                .forEach(
                        tableId -> {
                            Table table = tables.forTable(tableId);
                            Objects.requireNonNull(
                                    table, "Table should not be null for tableId: " + tableId);

                            // Set events to populate proper sourceInfo into offsetContext
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

                            for (TableChanges.TableChange tableChange :
                                    schemaChangeEvent.getTableChanges()) {
                                this.schemasByTableId.put(tableId, tableChange);
                            }
                        });

        allSchemasLoaded = true;
    }
}
