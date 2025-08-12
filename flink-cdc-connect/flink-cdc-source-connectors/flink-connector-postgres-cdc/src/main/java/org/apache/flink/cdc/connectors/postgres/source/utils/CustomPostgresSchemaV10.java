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

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A CustomPostgresSchema specifically designed for PostgreSQL 10. This version handles the specific
 * capabilities and limitations of PostgreSQL 10, particularly around partition table support and
 * schema reading.
 */
public class CustomPostgresSchemaV10 extends AbstractCustomPostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPostgresSchemaV10.class);

    public CustomPostgresSchemaV10(
            PostgresConnection jdbcConnection, PostgresSourceConfig sourceConfig) {
        super(jdbcConnection, sourceConfig);
    }

    @Override
    protected void readSchemaWithVersionSpecificHandling(
            Tables tables, List<TableId> targetTableIds) throws SQLException {
        // Use cached schema reading for PostgreSQL 10 to minimize database calls
        Tables cachedTables =
                PostgresSchemaCache.getInstance()
                        .getOrLoadTables(
                                jdbcConnection,
                                dbzConfig.databaseName(),
                                null, // Read all schemas
                                dbzConfig.getTableFilters().dataCollectionFilter(),
                                false // Don't force refresh
                                );

        // Copy relevant tables to the provided Tables object
        for (TableId tableId : targetTableIds) {
            Table table = cachedTables.forTable(tableId);
            if (table != null) {
                tables.overwriteTable(table);
            }
            // Handle partition tables specifically for PG10
            handlePartitionTableForPG10(tables, tableId);
        }
    }

    @Override
    protected Table handleMissingTable(TableId tableId, Tables tables) throws SQLException {
        LOG.warn("Table {} not found in schema, attempting partition resolution", tableId);

        // Check if this is a partition table and try to resolve via parent table
        if (PostgresVersionUtils.isPartitionTable(jdbcConnection, tableId)) {
            Optional<TableId> parentTableId =
                    PostgresVersionUtils.findParentTableForPartition(jdbcConnection, tableId);
            if (parentTableId.isPresent()) {
                Table parentTable = tables.forTable(parentTableId.get());
                if (parentTable != null) {
                    LOG.debug(
                            "Resolved partition table {} via parent table {}",
                            tableId,
                            parentTableId.get());
                    return parentTable;
                }
            }
        }

        // Fallback to individual table read
        return readIndividualTable(tableId);
    }

    @Override
    protected Table enhanceTableForVersion(Table table, TableId tableId) throws SQLException {
        // PG10 specific: Fix missing primary keys for partition tables
        return enhanceTableWithPrimaryKeyForPG10(table, tableId);
    }

    @Override
    protected SchemaChangeEvent createSchemaChangeEvent(
            PostgresPartition partition,
            PostgresOffsetContext offsetContext,
            Table table,
            TableId tableId) {

        // PG10 might need special handling for certain table types
        boolean isFromSnapshot = shouldTreatAsSnapshot(table, tableId);

        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                dbzConfig.databaseName(),
                tableId.schema(),
                null,
                table,
                isFromSnapshot);
    }

    /**
     * Handle partition tables with PostgreSQL 10 specific logic. PG10 introduced partition tables
     * but they have different system catalog structure compared to PG11+ versions.
     */
    private void handlePartitionTableForPG10(Tables tables, TableId tableId) throws SQLException {
        String checkPartitionSql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PARTITION_TABLE_WITH_METADATA,
                        jdbcConnection);

        try (Connection conn = jdbcConnection.connection();
                PreparedStatement stmt = conn.prepareStatement(checkPartitionSql)) {

            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    boolean isPartition = rs.getBoolean("relispartition");
                    String relKind = rs.getString("relkind");

                    if (isPartition || "p".equals(relKind)) {
                        LOG.debug(
                                "Table {} is a partition table in PG10, applying special handling",
                                tableId);
                        handlePG10PartitionSpecificLogic(tables, tableId);
                    }
                }
            }
        }
    }

    /**
     * Apply PostgreSQL 10 specific logic for partition tables. This includes handling the different
     * way PG10 manages partition metadata.
     */
    private void handlePG10PartitionSpecificLogic(Tables tables, TableId tableId) {
        Table existingTable = tables.forTable(tableId);
        if (existingTable != null) {
            LOG.debug("Applying PG10 partition compatibility adjustments for table {}", tableId);
            // In PG10, partition tables might need special handling for:
            // 1. Inheritance relationships
            // 2. Constraint handling
            // 3. Index management
            // For now, we log and continue with the existing table structure
            // Additional customization can be added here as needed
        }
    }

    /** Determine if a table should be treated as snapshot based on PG10 characteristics. */
    private boolean shouldTreatAsSnapshot(Table table, TableId tableId) {
        // For PG10, we might need different logic for determining snapshot behavior
        // This is particularly important for partition tables which had limited support in PG10
        try {
            return !PostgresVersionUtils.isPartitionedTable(jdbcConnection, tableId);
        } catch (Exception e) {
            LOG.warn(
                    "Could not determine partition status for table {}, defaulting to snapshot=true",
                    tableId,
                    e);
            return true;
        }
    }

    /**
     * Enhance table with primary key information for PostgreSQL 10 partition tables. In PG10,
     * partition tables don't inherit primary key constraints automatically, so we need to manually
     * detect and add them from the parent table.
     */
    private Table enhanceTableWithPrimaryKeyForPG10(Table table, TableId tableId)
            throws SQLException {
        // If table already has primary keys, no need to enhance
        if (!table.primaryKeyColumns().isEmpty()) {
            return table;
        }

        // Use PostgresVersionUtils to check if this is a partition table
        if (!PostgresVersionUtils.isPartitionTable(jdbcConnection, tableId)) {
            return table;
        }

        LOG.debug(
                "Table {} is a partition in PG10 and has no primary key, attempting to inherit from parent",
                tableId);

        // Find the parent table using PostgresVersionUtils
        Optional<TableId> parentTableId =
                PostgresVersionUtils.findParentTableForPartition(jdbcConnection, tableId);
        if (!parentTableId.isPresent()) {
            LOG.warn("Could not find parent table for partition {}", tableId);
            return table;
        }

        // Get primary key columns from parent table using unified utils
        List<String> parentPrimaryKeyNames =
                PostgresVersionUtils.getPrimaryKeyColumns(jdbcConnection, parentTableId.get());
        if (parentPrimaryKeyNames.isEmpty()) {
            LOG.debug("Parent table {} has no primary keys", parentTableId.get());
            return table;
        }

        // Enhance the table with inherited primary keys
        return addPrimaryKeysToTable(table, parentPrimaryKeyNames, tableId);
    }

    /** Add primary key information to a table. */
    private Table addPrimaryKeysToTable(
            Table originalTable, List<String> primaryKeyNames, TableId tableId) {
        // Create a new table editor to modify the table
        TableEditor tableEditor = originalTable.edit();

        // Add primary key constraint
        List<String> validPrimaryKeys = new ArrayList<>();
        for (String pkName : primaryKeyNames) {
            if (originalTable.columnWithName(pkName) != null) {
                validPrimaryKeys.add(pkName);
            } else {
                LOG.warn(
                        "Primary key column '{}' from parent table not found in partition table {}",
                        pkName,
                        tableId);
            }
        }

        if (!validPrimaryKeys.isEmpty()) {
            tableEditor.setPrimaryKeyNames(validPrimaryKeys);
            LOG.info(
                    "Added {} primary key columns to partition table {}: {}",
                    validPrimaryKeys.size(),
                    tableId,
                    validPrimaryKeys);
        }

        return tableEditor.create();
    }

    @Override
    protected void readTableSchemaWithTypeInfo(
            List<TableId> tableIds, java.util.Map<TableId, PostgresTableType> tableTypes)
            throws SQLException {

        LOG.debug(
                "Reading schema for {} tables with pre-computed type information in PG10",
                tableIds.size());

        Tables tables = new Tables();

        // Separate tables by type for optimized processing
        List<TableId> regularTables = new ArrayList<>();
        List<TableId> partitionedTables = new ArrayList<>();
        List<TableId> partitionTables = new ArrayList<>();

        for (TableId tableId : tableIds) {
            PostgresTableType tableType = tableTypes.get(tableId);
            if (tableType != null) {
                switch (tableType) {
                    case TABLE:
                        regularTables.add(tableId);
                        break;
                    case PARTITIONED_TABLE:
                        partitionedTables.add(tableId);
                        break;
                    default:
                        // For UNKNOWN or other types, check if it might be a partition
                        if (isPartitionedTable(tableId)) {
                            partitionTables.add(tableId);
                        } else {
                            regularTables.add(tableId);
                        }
                        break;
                }
            } else {
                // No type information available, add to regular processing
                regularTables.add(tableId);
            }
        }

        // Process regular tables in bulk (most efficient)
        if (!regularTables.isEmpty()) {
            LOG.debug("Processing {} regular tables in bulk", regularTables.size());
            jdbcConnection.readSchema(
                    tables,
                    dbzConfig.databaseName(),
                    null,
                    dbzConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }

        // Process partitioned tables with special handling
        if (!partitionedTables.isEmpty()) {
            LOG.debug(
                    "Processing {} partitioned tables with enhanced handling",
                    partitionedTables.size());
            for (TableId tableId : partitionedTables) {
                handlePartitionTableForPG10WithTypeInfo(
                        tables, tableId, PostgresTableType.PARTITIONED_TABLE);
            }
        }

        // Process partition tables (child partitions) with enhanced handling
        if (!partitionTables.isEmpty()) {
            LOG.debug(
                    "Processing {} partition tables with enhanced handling",
                    partitionTables.size());
            for (TableId tableId : partitionTables) {
                handlePartitionTableForPG10WithTypeInfo(tables, tableId, PostgresTableType.TABLE);
            }
        }
    }

    /**
     * Enhanced partition table handling that leverages pre-computed table type information. This
     * version avoids redundant database queries for partition detection.
     */
    private void handlePartitionTableForPG10WithTypeInfo(
            Tables tables, TableId tableId, PostgresTableType tableType) throws SQLException {

        // Skip partition detection query since we already know the type
        if (tableType == PostgresTableType.PARTITIONED_TABLE) {
            LOG.debug(
                    "Table {} is known to be partitioned, applying PG10 partition logic", tableId);
            handlePG10PartitionSpecificLogic(tables, tableId);
        } else if (tableType == PostgresTableType.TABLE && isPartitionedTable(tableId)) {
            LOG.debug(
                    "Table {} is known to be a partition, applying PG10 partition logic", tableId);
            handlePG10PartitionSpecificLogic(tables, tableId);
        } else {
            // Regular table, process normally
            Table table = readIndividualTable(tableId);
            if (table != null) {
                tables.overwriteTable(table);
            }
        }
    }
}
