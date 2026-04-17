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
 * Manages PostgreSQL publication membership for PG10 partitioned tables.
 *
 * <p>Handles all publication-related SQL operations: querying members, validating membership, and
 * adding new tables to publications.
 */
public final class Pg10PublicationManager {

    /**
     * Identifies child partitions that are NOT in the publication. Unlike {@link
     * #validatePublicationMembership}, this method returns a list instead of throwing, allowing the
     * caller to reconcile.
     */
    public static List<TableId> findMissingPublicationChildren(
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
     * Adds child partitions to the publication at runtime. Used when new child partitions are
     * discovered after CDC job startup.
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
                } catch (SQLException e) {
                    throw new SQLException(
                            String.format(
                                    "Failed to add table '%s' to publication '%s' using SQL [%s]",
                                    tableId, publicationName, sql),
                            e.getSQLState(),
                            e.getErrorCode(),
                            e);
                }
            }
        }
    }

    /**
     * Validates that all child partitions are members of the specified publication.
     *
     * @throws Pg10PartitionMapper.PartitionPublicationValidationException if any child partition is
     *     missing
     */
    public static void validatePublicationMembership(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        if (childTableIds == null || childTableIds.isEmpty()) {
            return;
        }

        List<TableId> missingChildren =
                findMissingPublicationChildren(jdbc, publicationName, childTableIds);

        if (!missingChildren.isEmpty()) {
            String missingChildrenStr =
                    missingChildren.stream()
                            .map(TableId::toString)
                            .collect(Collectors.joining(", "));
            throw new Pg10PartitionMapper.PartitionPublicationValidationException(
                    String.format(
                            "PG10 partition publication validation failed: child partitions [%s] are not members of publication '%s'",
                            missingChildrenStr, publicationName));
        }
    }

    private static Set<TableId> queryPublicationTables(JdbcConnection jdbc, String publicationName)
            throws SQLException {
        Set<TableId> publishedTables = new HashSet<>();
        String publicationTablesQuery =
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?";
        try (PreparedStatement stmt = jdbc.connection().prepareStatement(publicationTablesQuery)) {
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
}
