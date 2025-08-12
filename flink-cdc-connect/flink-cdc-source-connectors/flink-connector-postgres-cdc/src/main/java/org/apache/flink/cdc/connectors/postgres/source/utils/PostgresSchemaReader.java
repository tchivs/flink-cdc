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
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.List;
import java.util.Map;

/**
 * Interface for PostgreSQL schema readers. This allows different implementations for different
 * PostgreSQL versions.
 */
public interface PostgresSchemaReader {

    /**
     * Get the schema for a single table.
     *
     * @param tableId the table identifier
     * @return the table change representing the schema
     */
    TableChange getTableSchema(TableId tableId);

    /**
     * Get the schemas for multiple tables.
     *
     * @param tableIds list of table identifiers
     * @return map of table identifiers to their schema changes
     */
    Map<TableId, TableChange> getTableSchema(List<TableId> tableIds);

    /**
     * Get the schema for a single table with pre-computed table type information. This method
     * allows optimized schema reading by avoiding additional database queries to determine table
     * types.
     *
     * @param tableInfo the table information including identifier and type
     * @return the table change representing the schema
     */
    default TableChange getTableSchema(TableInfo tableInfo) {
        // Default implementation delegates to the basic method for backward compatibility
        return getTableSchema(tableInfo.getTableId());
    }

    /**
     * Get the schemas for multiple tables with pre-computed table type information. This method
     * provides enhanced performance by utilizing table type information to optimize partition
     * handling and schema discovery.
     *
     * @param tableInfos list of table information objects including identifiers and types
     * @return map of table identifiers to their schema changes
     */
    default Map<TableId, TableChange> getTableSchema(java.util.Set<TableInfo> tableInfos) {
        // Default implementation extracts table IDs and delegates to the basic method
        List<TableId> tableIds =
                tableInfos.stream()
                        .map(TableInfo::getTableId)
                        .collect(java.util.stream.Collectors.toList());
        return getTableSchema(tableIds);
    }

    /**
     * Sets the table type cache to optimize subsequent schema operations. This method allows
     * pre-populating table type information to avoid redundant database queries during schema
     * reading.
     *
     * @param tableTypes map of table identifiers to their types
     */
    default void setTableTypeCache(Map<TableId, PostgresTableType> tableTypes) {
        // Default implementation does nothing - override in implementations that support caching
    }
}
