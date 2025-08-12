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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for CustomPostgresSchema implementations. This eliminates code duplication
 * between different PostgreSQL version-specific implementations.
 */
public abstract class AbstractCustomPostgresSchema implements PostgresSchemaReader {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractCustomPostgresSchema.class);

    // cache the schema for each table
    protected final Map<TableId, TableChange> schemasByTableId = new HashMap<>();

    // cache for table type information to optimize partition detection
    protected final Map<TableId, PostgresTableType> tableTypeCache = new HashMap<>();

    protected final PostgresConnection jdbcConnection;
    protected final PostgresConnectorConfig dbzConfig;
    protected final PostgresSourceConfig sourceConfig;

    protected AbstractCustomPostgresSchema(
            PostgresConnection jdbcConnection, PostgresSourceConfig sourceConfig) {
        this.jdbcConnection = jdbcConnection;
        this.sourceConfig = sourceConfig;
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
    }

    @Override
    public final TableChange getTableSchema(TableId tableId) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                // OPTIMIZATION: Use bulk schema reading to cache multiple tables at once
                // This addresses the N+1 problem where PostgreSQL API returns all table schemas
                // but we were only caching the requested one
                readTableSchema(Collections.singletonList(tableId));
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    @Override
    public final Map<TableId, TableChange> getTableSchema(List<TableId> tableIds) {
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

    /** Template method for reading table schemas. Subclasses implement version-specific logic. */
    private List<TableChange> readTableSchema(List<TableId> tableIds) throws SQLException {
        List<TableChange> tableChanges = new ArrayList<>();

        final PostgresOffsetContext offsetContext =
                PostgresOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());

        // Use cached schema reading to minimize database calls
        Tables tables =
                PostgresSchemaCache.getInstance()
                        .getOrLoadTables(
                                jdbcConnection,
                                dbzConfig.databaseName(),
                                null, // Read all schemas
                                dbzConfig.getTableFilters().dataCollectionFilter(),
                                false // Don't force refresh
                                );

        // OPTIMIZATION: Bulk cache all available tables from the API response
        // The PostgreSQL API returns schema for all matching tables, so we should
        // cache all of them to avoid redundant API calls for subsequent tables
        cacheAllAvailableSchemas(tables, partition, offsetContext);

        for (TableId tableId : tableIds) {
            Table table = tables.forTable(tableId);
            if (table == null) {
                LOG.warn("Table {} not found in schema, attempting resolution", tableId);
                table = handleMissingTable(tableId, tables);
                if (table == null) {
                    throw new FlinkRuntimeException(
                            String.format("Could not read schema for table %s", tableId));
                }
                tables.overwriteTable(table);
            }

            // Version-specific table enhancement (e.g., primary key inheritance)
            table = enhanceTableForVersion(table, tableId);

            // Check if already cached from bulk operation
            if (!schemasByTableId.containsKey(tableId)) {
                // set the events to populate proper sourceInfo into offsetContext
                offsetContext.event(tableId, Instant.now());

                // Create schema change event with version-specific handling
                SchemaChangeEvent schemaChangeEvent =
                        createSchemaChangeEvent(partition, offsetContext, table, tableId);

                for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                    this.schemasByTableId.put(tableId, tableChange);
                }
            }
            tableChanges.add(this.schemasByTableId.get(tableId));
        }
        return tableChanges;
    }

    /**
     * Bulk cache all available table schemas from the PostgreSQL API response. This optimization
     * addresses the N+1 problem where each getTableSchema() call triggers a full schema read even
     * though the API returns all table schemas.
     *
     * <p>Based on PR #2571 discussion: when PostgreSQL API retrieves schema for one table, it
     * actually returns schemas for ALL matching tables. We should cache all of them to avoid
     * redundant API calls.
     */
    private void cacheAllAvailableSchemas(
            Tables tables, PostgresPartition partition, PostgresOffsetContext offsetContext)
            throws SQLException {

        int cachedCount = 0;
        LOG.debug("Starting bulk schema caching for {} available tables", tables.size());

        // Iterate through all tables returned by the PostgreSQL API
        for (TableId tableId : tables.tableIds()) {
            // Skip if already cached
            if (schemasByTableId.containsKey(tableId)) {
                continue;
            }

            Table table = tables.forTable(tableId);
            if (table == null) {
                continue;
            }

            try {
                // Version-specific table enhancement (e.g., primary key inheritance)
                table = enhanceTableForVersion(table, tableId);

                // Update tables object with enhanced table
                tables.overwriteTable(table);

                // set the events to populate proper sourceInfo into offsetContext
                offsetContext.event(tableId, Instant.now());

                // Create schema change event with version-specific handling
                SchemaChangeEvent schemaChangeEvent =
                        createSchemaChangeEvent(partition, offsetContext, table, tableId);

                for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                    this.schemasByTableId.put(tableId, tableChange);
                    cachedCount++;
                }

            } catch (SQLException e) {
                LOG.warn(
                        "Failed to cache schema for table {}, will process individually: {}",
                        tableId,
                        e.getMessage());
                // Continue with other tables, individual processing will handle this table
            }
        }

        if (cachedCount > 0) {
            LOG.info("Bulk cached {} table schemas to avoid redundant API calls", cachedCount);
        }
    }

    // Template methods to be implemented by subclasses

    /**
     * Version-specific schema reading logic. Subclasses implement their specific approach to
     * reading schemas.
     */
    protected abstract void readSchemaWithVersionSpecificHandling(
            Tables tables, List<TableId> targetTableIds) throws SQLException;

    /** Handle missing table case. Different versions may have different strategies. */
    protected abstract Table handleMissingTable(TableId tableId, Tables tables) throws SQLException;

    /** Version-specific table enhancement (e.g., primary key inheritance for PG10). */
    protected abstract Table enhanceTableForVersion(Table table, TableId tableId)
            throws SQLException;

    /** Create schema change event with version-specific considerations. */
    protected abstract SchemaChangeEvent createSchemaChangeEvent(
            PostgresPartition partition,
            PostgresOffsetContext offsetContext,
            Table table,
            TableId tableId);

    // Common utility methods that can be shared

    /**
     * Read an individual table when it's not found in the bulk schema read. This is a common
     * fallback mechanism with caching optimization.
     */
    protected Table readIndividualTable(TableId tableId) throws SQLException {
        try {
            // Check if table schema is already cached
            if (PostgresSchemaCache.getInstance()
                    .isTableSchemaCached(tableId, jdbcConnection, dbzConfig.databaseName())) {
                Tables cachedTables =
                        PostgresSchemaCache.getInstance()
                                .getOrLoadTables(
                                        jdbcConnection,
                                        dbzConfig.databaseName(),
                                        tableId.schema(),
                                        dbzConfig.getTableFilters().dataCollectionFilter(),
                                        false);

                Table cachedTable = cachedTables.forTable(tableId);
                if (cachedTable != null) {
                    LOG.debug("Individual table {} found in cache", tableId);
                    return cachedTable;
                }
            }

            // Cache miss or table not found - perform individual read with cache
            LOG.debug("Reading individual table schema for {}", tableId);
            Tables.TableFilter specificFilter =
                    Tables.TableFilter.fromPredicate(id -> id.equals(tableId));

            Tables individualTables =
                    PostgresSchemaCache.getInstance()
                            .getOrLoadTables(
                                    jdbcConnection,
                                    dbzConfig.databaseName(),
                                    tableId.schema(),
                                    specificFilter,
                                    false);

            return individualTables.forTable(tableId);
        } catch (SQLException e) {
            LOG.error("Failed to read individual table schema for {}", tableId, e);
            return null;
        }
    }

    /**
     * Create a basic schema change event. Subclasses can override for version-specific behavior.
     */
    protected SchemaChangeEvent createBasicSchemaChangeEvent(
            PostgresPartition partition,
            PostgresOffsetContext offsetContext,
            Table table,
            TableId tableId) {

        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                dbzConfig.databaseName(),
                tableId.schema(),
                null,
                table,
                true);
    }

    // Getters for subclass access
    protected PostgresConnection getJdbcConnection() {
        return jdbcConnection;
    }

    protected PostgresConnectorConfig getDbzConfig() {
        return dbzConfig;
    }

    protected PostgresSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    // Enhanced methods using table type information

    @Override
    public void setTableTypeCache(Map<TableId, PostgresTableType> tableTypes) {
        this.tableTypeCache.putAll(tableTypes);
        LOG.debug("Set table type cache for {} tables in schema reader", tableTypes.size());
    }

    @Override
    public TableChange getTableSchema(TableInfo tableInfo) {
        // Cache the table type information for future use
        tableTypeCache.put(tableInfo.getTableId(), tableInfo.getTableType());

        // Use the enhanced logic that can leverage table type information
        return getTableSchemaWithTypeInfo(tableInfo.getTableId(), tableInfo.getTableType());
    }

    @Override
    public Map<TableId, TableChange> getTableSchema(java.util.Set<TableInfo> tableInfos) {
        // Pre-populate table type cache with all provided information
        Map<TableId, PostgresTableType> typeMap =
                tableInfos.stream()
                        .collect(
                                java.util.stream.Collectors.toMap(
                                        TableInfo::getTableId, TableInfo::getTableType));
        setTableTypeCache(typeMap);

        // Extract table IDs and process with enhanced logic
        List<TableId> tableIds =
                tableInfos.stream()
                        .map(TableInfo::getTableId)
                        .collect(java.util.stream.Collectors.toList());

        return getTableSchema(tableIds);
    }

    /**
     * Enhanced table schema reading that leverages pre-computed table type information. This method
     * can make optimizations based on whether the table is partitioned.
     */
    protected TableChange getTableSchemaWithTypeInfo(TableId tableId, PostgresTableType tableType) {
        // Check cache first
        if (schemasByTableId.containsKey(tableId)) {
            return schemasByTableId.get(tableId);
        }

        try {
            // Use table type information for optimized schema reading
            readTableSchemaWithTypeInfo(
                    Collections.singletonList(tableId),
                    Collections.singletonMap(tableId, tableType));
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read table schema with type info", e);
        }

        return schemasByTableId.get(tableId);
    }

    /**
     * Enhanced schema reading method that can utilize table type information for optimization.
     * Subclasses can override this for version-specific optimizations.
     */
    protected void readTableSchemaWithTypeInfo(
            List<TableId> tableIds, Map<TableId, PostgresTableType> tableTypes)
            throws SQLException {
        // Default implementation falls back to standard method
        // Subclasses can override for optimized handling
        readTableSchema(tableIds);
    }

    /**
     * Gets the cached table type for a given table.
     *
     * @param tableId the table identifier
     * @return the cached table type, or null if not cached
     */
    protected PostgresTableType getCachedTableType(TableId tableId) {
        return tableTypeCache.get(tableId);
    }

    /**
     * Checks if a table is known to be partitioned based on cached information.
     *
     * @param tableId the table identifier
     * @return true if the table is cached as partitioned, false otherwise
     */
    protected boolean isPartitionedTable(TableId tableId) {
        PostgresTableType tableType = tableTypeCache.get(tableId);
        return tableType != null && tableType.isPartitioned();
    }

    /**
     * Checks if a table supports CDC based on cached type information.
     *
     * @param tableId the table identifier
     * @return true if the table type supports CDC, false otherwise
     */
    protected boolean isCdcSupportedTable(TableId tableId) {
        PostgresTableType tableType = tableTypeCache.get(tableId);
        return tableType != null && tableType.isCdcSupported();
    }
}
