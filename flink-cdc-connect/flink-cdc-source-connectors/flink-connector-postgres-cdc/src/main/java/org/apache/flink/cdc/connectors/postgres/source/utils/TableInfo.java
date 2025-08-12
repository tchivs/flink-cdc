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

import io.debezium.relational.TableId;

import java.util.Objects;

/**
 * Represents a PostgreSQL table with its associated type information.
 *
 * <p>This class combines a {@link TableId} with a {@link PostgresTableType} to provide efficient
 * table type checking without requiring additional database queries.
 *
 * <p>This class is immutable and thread-safe.
 */
public class TableInfo implements Comparable<TableInfo> {

    private final TableId tableId;
    private final PostgresTableType tableType;

    /**
     * Creates a new TableInfo instance.
     *
     * @param tableId the table identifier
     * @param tableType the PostgreSQL table type
     * @throws IllegalArgumentException if either parameter is null
     */
    public TableInfo(TableId tableId, PostgresTableType tableType) {
        this.tableId = Objects.requireNonNull(tableId, "tableId must not be null");
        this.tableType = Objects.requireNonNull(tableType, "tableType must not be null");
    }

    /**
     * Gets the table identifier.
     *
     * @return the table ID
     */
    public TableId getTableId() {
        return tableId;
    }

    /**
     * Gets the PostgreSQL table type.
     *
     * @return the table type
     */
    public PostgresTableType getTableType() {
        return tableType;
    }

    /**
     * Checks if this table is a partitioned table.
     *
     * @return true if this is a partitioned table
     */
    public boolean isPartitionedTable() {
        return tableType.isPartitioned();
    }

    /**
     * Checks if this table is a regular table.
     *
     * @return true if this is a regular table (not partitioned)
     */
    public boolean isRegularTable() {
        return tableType == PostgresTableType.TABLE;
    }

    /**
     * Checks if CDC operations are supported for this table.
     *
     * @return true if CDC operations are supported
     */
    public boolean isCdcSupported() {
        return tableType.isCdcSupported();
    }

    /**
     * Checks if this table is a view (regular or materialized).
     *
     * @return true if this is a view
     */
    public boolean isView() {
        return tableType == PostgresTableType.VIEW
                || tableType == PostgresTableType.MATERIALIZED_VIEW;
    }

    /**
     * Gets the schema name of the table.
     *
     * @return the schema name
     */
    public String getSchemaName() {
        return tableId.schema();
    }

    /**
     * Gets the table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableId.table();
    }

    /**
     * Gets the catalog name of the table.
     *
     * @return the catalog name
     */
    public String getCatalogName() {
        return tableId.catalog();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableInfo)) {
            return false;
        }
        TableInfo tableInfo = (TableInfo) o;
        return Objects.equals(tableId, tableInfo.tableId) && tableType == tableInfo.tableType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableType);
    }

    @Override
    public String toString() {
        return String.format("TableInfo{tableId=%s, type=%s}", tableId, tableType);
    }

    @Override
    public int compareTo(TableInfo other) {
        if (other == null) {
            return 1;
        }

        // First compare by table ID
        int tableIdComparison = this.tableId.compareTo(other.tableId);
        if (tableIdComparison != 0) {
            return tableIdComparison;
        }

        // If table IDs are equal, compare by table type
        return this.tableType.compareTo(other.tableType);
    }

    /**
     * Creates a TableInfo from a TableId with UNKNOWN type. This is useful for backward
     * compatibility.
     *
     * @param tableId the table identifier
     * @return a new TableInfo with UNKNOWN type
     */
    public static TableInfo withUnknownType(TableId tableId) {
        return new TableInfo(tableId, PostgresTableType.UNKNOWN);
    }

    /**
     * Creates a TableInfo for a regular table.
     *
     * @param tableId the table identifier
     * @return a new TableInfo for a regular table
     */
    public static TableInfo regularTable(TableId tableId) {
        return new TableInfo(tableId, PostgresTableType.TABLE);
    }

    /**
     * Creates a TableInfo for a partitioned table.
     *
     * @param tableId the table identifier
     * @return a new TableInfo for a partitioned table
     */
    public static TableInfo partitionedTable(TableId tableId) {
        return new TableInfo(tableId, PostgresTableType.PARTITIONED_TABLE);
    }
}
