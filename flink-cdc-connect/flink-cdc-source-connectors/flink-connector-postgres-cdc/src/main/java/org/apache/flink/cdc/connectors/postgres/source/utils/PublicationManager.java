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
import java.util.Collections;
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
     * Resolves the table list used by Debezium's filtered publication mode.
     *
     * <p>PostgreSQL 10 rejects declarative partitioned parent tables in publication membership.
     * When partition routing is enabled, the connector-level table filter may still intentionally
     * match those parents for schema registration and event routing, so only the publication SQL
     * should be rewritten: add concrete child partitions and remove partitioned parents.
     */
    public static Set<TableId> resolveFilteredPublicationTables(
            JdbcConnection jdbc, String publicationName, Set<TableId> capturedTables)
            throws SQLException {
        Set<TableId> publicationTables = new HashSet<>(capturedTables);
        Set<TableId> partitionedParents = filterPartitionedParents(jdbc, publicationTables);
        if (partitionedParents.isEmpty()) {
            return publicationTables;
        }

        Set<TableId> childPartitions = resolveChildPartitions(jdbc, partitionedParents);
        if (!childPartitions.isEmpty()) {
            LOG.info(
                    "Adding {} child partition table(s) to filtered publication '{}': {}",
                    childPartitions.size(),
                    publicationName,
                    childPartitions);
            publicationTables.addAll(childPartitions);
        }

        partitionedParents = filterPartitionedParents(jdbc, publicationTables);
        LOG.info(
                "Excluding {} partitioned parent table(s) from filtered publication '{}': {}",
                partitionedParents.size(),
                publicationName,
                partitionedParents);
        publicationTables.removeAll(partitionedParents);
        return publicationTables;
    }

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
     * Adds tables to the publication at runtime using a single batched ALTER PUBLICATION statement.
     * Falls back to per-table statements if the batch fails (e.g. partial duplicates), gracefully
     * handling tables that are already members (SQL state 42710).
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

        // Try batch ALTER PUBLICATION ADD TABLE first — single round-trip for all tables.
        try (Statement statement = jdbc.connection().createStatement()) {
            StringBuilder sql =
                    new StringBuilder(
                            String.format(
                                    "ALTER PUBLICATION %s ADD TABLE ",
                                    quoteIdentifier(publicationName)));
            boolean first = true;
            for (TableId tableId : tableIds) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(tableId.schema()))
                        .append('.')
                        .append(quoteIdentifier(tableId.table()));
                first = false;
            }
            statement.execute(sql.toString());
            LOG.info(
                    "Added {} table(s) to publication '{}' in a single batch",
                    tableIds.size(),
                    publicationName);
            return;
        } catch (SQLException e) {
            // 42710 = duplicate_object (one or more tables already in publication).
            // Fall through to per-table approach to handle partial duplicates gracefully.
            if (!"42710".equals(e.getSQLState())) {
                throw new SQLException(
                        String.format(
                                "Failed to batch-add %d table(s) to publication '%s'",
                                tableIds.size(), publicationName),
                        e.getSQLState(),
                        e.getErrorCode(),
                        e);
            }
            LOG.debug(
                    "Batch ALTER PUBLICATION hit duplicate_object; falling back to per-table adds");
        }

        // Fallback: add tables one-by-one, skipping duplicates
        int added = 0;
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
                    added++;
                    LOG.debug("Added table '{}' to publication '{}'", tableId, publicationName);
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
                "Publication membership update complete [publication={}, tablesAdded={}/{}]",
                publicationName,
                added,
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

    private static Set<TableId> resolveChildPartitions(
            JdbcConnection jdbc, Set<TableId> partitionedParents) throws SQLException {
        List<TableId> parentsWithSchema = tablesWithSchema(partitionedParents);
        if (parentsWithSchema.isEmpty()) {
            return Collections.emptySet();
        }

        String query =
                "WITH RECURSIVE "
                        + "requested_parent(parent_schema, parent_table, parent_catalog) AS (VALUES "
                        + valuePlaceholders(parentsWithSchema.size(), 3)
                        + "), "
                        + "parent_oid AS ("
                        + "SELECT c.oid, rp.parent_schema, rp.parent_table, rp.parent_catalog "
                        + "FROM requested_parent rp "
                        + "JOIN pg_namespace n ON n.nspname = rp.parent_schema "
                        + "JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = rp.parent_table"
                        + "), "
                        + "partition_tree(child_oid, parent_schema, parent_table, parent_catalog) AS ("
                        + "SELECT i.inhrelid, po.parent_schema, po.parent_table, po.parent_catalog "
                        + "FROM pg_inherits i "
                        + "JOIN parent_oid po ON po.oid = i.inhparent "
                        + "UNION "
                        + "SELECT i.inhrelid, pt.parent_schema, pt.parent_table, pt.parent_catalog "
                        + "FROM pg_inherits i "
                        + "JOIN partition_tree pt ON pt.child_oid = i.inhparent"
                        + ") "
                        + "SELECT cn.nspname AS child_schema, c.relname AS child_table, "
                        + "pt.parent_catalog "
                        + "FROM partition_tree pt "
                        + "JOIN pg_class c ON c.oid = pt.child_oid "
                        + "JOIN pg_namespace cn ON cn.oid = c.relnamespace";

        Set<TableId> childPartitions = new HashSet<>();
        try (PreparedStatement stmt = jdbc.connection().prepareStatement(query)) {
            bindTables(stmt, parentsWithSchema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    childPartitions.add(
                            new TableId(
                                    rs.getString("parent_catalog"),
                                    rs.getString("child_schema"),
                                    rs.getString("child_table")));
                }
            }
        }
        return childPartitions;
    }

    private static Set<TableId> filterPartitionedParents(
            JdbcConnection jdbc, Set<TableId> candidates) throws SQLException {
        List<TableId> candidatesWithSchema = tablesWithSchema(candidates);
        if (candidatesWithSchema.isEmpty()) {
            return Collections.emptySet();
        }

        String query =
                "WITH candidate(schema_name, table_name, catalog_name) AS (VALUES "
                        + valuePlaceholders(candidatesWithSchema.size(), 3)
                        + ") "
                        + "SELECT candidate.catalog_name, candidate.schema_name, candidate.table_name "
                        + "FROM candidate "
                        + "JOIN pg_namespace n ON n.nspname = candidate.schema_name "
                        + "JOIN pg_class c ON c.relnamespace = n.oid "
                        + "AND c.relname = candidate.table_name "
                        + "WHERE c.relkind = 'p'";

        Set<TableId> partitionedParents = new HashSet<>();
        try (PreparedStatement stmt = jdbc.connection().prepareStatement(query)) {
            bindTables(stmt, candidatesWithSchema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    partitionedParents.add(
                            new TableId(
                                    rs.getString("catalog_name"),
                                    rs.getString("schema_name"),
                                    rs.getString("table_name")));
                }
            }
        }
        return partitionedParents;
    }

    private static List<TableId> tablesWithSchema(Collection<TableId> tableIds) {
        return tableIds.stream()
                .filter(tableId -> tableId.schema() != null)
                .collect(Collectors.toList());
    }

    private static String valuePlaceholders(int rows, int columns) {
        String rowPlaceholder =
                "(" + String.join(", ", Collections.nCopies(columns, "CAST(? AS text)")) + ")";
        return String.join(", ", Collections.nCopies(rows, rowPlaceholder));
    }

    private static void bindTables(PreparedStatement stmt, List<TableId> tableIds)
            throws SQLException {
        int index = 1;
        for (TableId tableId : tableIds) {
            stmt.setString(index++, tableId.schema());
            stmt.setString(index++, tableId.table());
            stmt.setString(index++, tableId.catalog());
        }
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
