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

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages PostgreSQL publication membership for partitioned tables.
 *
 * <p>When partition routing is enabled, child partitions must be explicitly added to the
 * publication so that WAL events for those partitions are captured. This class handles querying
 * publication members, finding missing children, and adding tables to publications at runtime.
 *
 * <p>Works on all PostgreSQL versions (10+).
 */
public final class PublicationManager {

    private static final Logger LOG = LoggerFactory.getLogger(PublicationManager.class);

    private PublicationManager() {}

    /**
     * Finds child partitions that are NOT members of the publication.
     *
     * @param jdbc the JDBC connection
     * @param publicationName the publication name to check
     * @param childTableIds the expected child table IDs
     * @return list of TableIds missing from the publication (empty if all present)
     */
    public static List<TableId> findMissingChildren(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        if (childTableIds == null || childTableIds.isEmpty()) {
            return new ArrayList<>();
        }

        Set<TableId> publishedTables = queryPublicationTables(jdbc, publicationName);
        return childTableIds.stream()
                .filter(childId -> !publishedTables.contains(childId))
                .collect(Collectors.toList());
    }

    /**
     * Validates that all child partitions are members of the specified publication. Throws if any
     * are missing.
     *
     * @param jdbc the JDBC connection
     * @param publicationName the publication name
     * @param childTableIds child table IDs to validate
     * @throws PublicationValidationException if any children are not in the publication
     */
    public static void validateMembership(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        if (childTableIds == null || childTableIds.isEmpty()) {
            return;
        }

        List<TableId> missing = findMissingChildren(jdbc, publicationName, childTableIds);
        if (!missing.isEmpty()) {
            String missingStr =
                    missing.stream().map(TableId::toString).collect(Collectors.joining(", "));
            throw new PublicationValidationException(
                    String.format(
                            "Partition publication validation failed: child partitions [%s] "
                                    + "are not members of publication '%s'. "
                                    + "Add them with: ALTER PUBLICATION %s ADD TABLE <child>;",
                            missingStr, publicationName, publicationName));
        }
    }

    /**
     * Adds tables to the publication at runtime. Used when new child partitions are discovered
     * during streaming.
     *
     * @param jdbc the JDBC connection
     * @param publicationName the publication name
     * @param tableIds tables to add
     */
    public static void addTablesToPublication(
            JdbcConnection jdbc, String publicationName, Collection<TableId> tableIds)
            throws SQLException {
        if (tableIds == null || tableIds.isEmpty()) {
            return;
        }

        try (Statement statement = jdbc.connection().createStatement()) {
            for (TableId tableId : tableIds) {
                String sql =
                        String.format(
                                "ALTER PUBLICATION %s ADD TABLE %s.%s",
                                quoteIdentifier(publicationName),
                                quoteIdentifier(tableId.schema()),
                                quoteIdentifier(tableId.table()));
                try {
                    statement.execute(sql);
                    LOG.info("Added table '{}' to publication '{}'", tableId, publicationName);
                } catch (SQLException e) {
                    // 42710 = duplicate_object (table already in publication) — not an error
                    if ("42710".equals(e.getSQLState())) {
                        LOG.debug(
                                "Table '{}' already in publication '{}', skipping",
                                tableId,
                                publicationName);
                    } else {
                        throw new SQLException(
                                String.format(
                                        "Failed to add table '%s' to publication '%s'",
                                        tableId, publicationName),
                                e.getSQLState(),
                                e.getErrorCode(),
                                e);
                    }
                }
            }
        }

        LOG.info(
                "Publication membership update complete [publication={}, tablesAdded={}]",
                publicationName,
                tableIds.size());
    }

    /**
     * Queries the current members of a publication.
     *
     * @param jdbc the JDBC connection
     * @param publicationName the publication name
     * @return set of TableIds currently in the publication
     */
    public static Set<TableId> queryPublicationTables(JdbcConnection jdbc, String publicationName)
            throws SQLException {
        Set<TableId> publishedTables = new HashSet<>();
        String query = "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?";
        try (PreparedStatement stmt = jdbc.connection().prepareStatement(query)) {
            stmt.setString(1, publicationName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String schemaName = rs.getString("schemaname");
                    String tableName = rs.getString("tablename");
                    publishedTables.add(new TableId(null, schemaName, tableName));
                }
            }
        }
        return publishedTables;
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /** Exception thrown when partition publication validation fails. */
    public static class PublicationValidationException extends SQLException {
        private static final long serialVersionUID = 1L;

        public PublicationValidationException(String message) {
            super(message);
        }
    }
}
