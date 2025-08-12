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

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** A utility class for table discovery. */
public class TableDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    /**
     * Lists tables based on database, filtering by table filters.
     *
     * @param database The database to list tables from
     * @param jdbc JdbcConnection to interact with the database
     * @param tableFilters The relational table filters to apply
     * @return Unmodifiable set of captured TableIds after applying filters
     * @throws SQLException If an SQL error occurs during retrieval
     */
    public static Set<TableId> listTables(
            String database, JdbcConnection jdbc, @Nullable RelationalTableFilters tableFilters)
            throws SQLException {
        LOG.debug("Discovering tables for database: {}", database);

        // Retrieve all table ids, filtering by TABLE and PARTITIONED TABLE
        Set<TableId> allTableIds =
                jdbc.readTableNames(
                        database, null, null, new String[] {"TABLE", "PARTITIONED TABLE"});

        // Filter tables based on the data collection filter, if provided, otherwise return all
        // tables
        Set<TableId> capturedTables =
                (tableFilters == null)
                        ? new TreeSet<>(allTableIds) // Automatically sorts the tables
                        : allTableIds.stream()
                                .filter(
                                        tableId -> {
                                            if (tableFilters
                                                    .dataCollectionFilter()
                                                    .isIncluded(tableId)) {
                                                LOG.trace(
                                                        "Adding table {} to the list of captured tables",
                                                        tableId);
                                                return true;
                                            } else {
                                                LOG.trace(
                                                        "Ignoring table {} as it's not included in the filter configuration",
                                                        tableId);
                                                return false;
                                            }
                                        })
                                .collect(
                                        Collectors.toCollection(
                                                TreeSet::new)); // Ensure sorted order

        // Log the captured tables or warn if none were captured
        if (capturedTables.isEmpty()) {
            LOG.warn("No tables captured. Please check the table filters.");
        }
        return Collections.unmodifiableSet(capturedTables); // Return an unmodifiable set
    }

    /**
     * Lists tables with type information based on database, filtering by table filters.
     *
     * <p>This method provides enhanced table discovery by returning both table identifiers and
     * their types in a single database query, which improves performance by avoiding additional
     * partition detection queries.
     *
     * @param database The database to list tables from
     * @param jdbc PostgresConnection to interact with the database
     * @param tableFilters The relational table filters to apply
     * @return Unmodifiable set of captured TableInfo objects after applying filters
     * @throws SQLException If an SQL error occurs during retrieval
     */
    public static Set<TableInfo> listTablesWithType(
            String database, PostgresConnection jdbc, @Nullable RelationalTableFilters tableFilters)
            throws SQLException {

        LOG.debug("Discovering tables with type information for database: {}", database);

        // Retrieve all tables with type information in a single query
        Map<TableId, PostgresTableType> tablesWithType =
                jdbc.readTableNamesWithType(
                        database, null, null, PostgresTableType.getCdcSupportedTypes());

        // Convert to TableInfo and apply filters
        Set<TableInfo> capturedTables =
                tablesWithType.entrySet().stream()
                        .map(entry -> new TableInfo(entry.getKey(), entry.getValue()))
                        .filter(
                                tableInfo -> {
                                    if (tableFilters == null) {
                                        return true;
                                    }

                                    if (tableFilters
                                            .dataCollectionFilter()
                                            .isIncluded(tableInfo.getTableId())) {
                                        LOG.trace(
                                                "Adding table {} (type: {}) to captured tables",
                                                tableInfo.getTableId(),
                                                tableInfo.getTableType());
                                        return true;
                                    } else {
                                        LOG.trace(
                                                "Ignoring table {} (type: {}) due to filter",
                                                tableInfo.getTableId(),
                                                tableInfo.getTableType());
                                        return false;
                                    }
                                })
                        .collect(Collectors.toCollection(TreeSet::new));

        // Log the captured tables or warn if none were captured
        if (capturedTables.isEmpty()) {
            LOG.warn("No tables with type information captured. Please check the table filters.");
        } else {
            LOG.debug(
                    "Captured {} tables with type information for database: {}",
                    capturedTables.size(),
                    database);
        }

        return Collections.unmodifiableSet(capturedTables);
    }

    /**
     * Lists all tables with type information from the given databases.
     *
     * @param jdbc PostgresConnection to interact with the database
     * @param databases Varargs list of database names
     * @return Unmodifiable list of all TableInfo objects
     * @throws SQLException If an SQL error occurs during retrieval
     */
    public static List<TableInfo> listTablesWithType(PostgresConnection jdbc, String... databases)
            throws SQLException {
        List<TableInfo> tableInfos = new ArrayList<>();
        for (String database : databases) {
            tableInfos.addAll(listTablesWithType(database, jdbc, null));
        }
        return Collections.unmodifiableList(tableInfos);
    }
}
