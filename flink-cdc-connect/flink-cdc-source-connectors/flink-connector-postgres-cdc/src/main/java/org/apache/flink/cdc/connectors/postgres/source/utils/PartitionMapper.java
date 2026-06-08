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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for discovering parent↔child partition relationships via pg_inherits.
 *
 * <p>This is used when {@code includePartitionedTables=true} to discover child partitions of parent
 * tables and build routing mappings. Works on all PostgreSQL versions (10+).
 */
public final class PartitionMapper {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMapper.class);

    private PartitionMapper() {}

    /**
     * Discovers parent→child partition mappings by querying pg_inherits for the given parent
     * tables.
     *
     * @param parentTableIds the parent TableIds to discover children for
     * @param jdbc the JDBC connection
     * @param filterByParent if true, only returns children for the specified parent tables; if
     *     false, returns all partition relationships in the database
     * @return a map of parent TableId to list of child TableIds
     */
    public static Map<TableId, List<TableId>> discoverPartitionMappings(
            Collection<TableId> parentTableIds, JdbcConnection jdbc, boolean filterByParent)
            throws SQLException {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();

        String query;
        if (filterByParent && !parentTableIds.isEmpty()) {
            StringBuilder parentList = new StringBuilder();
            for (TableId parentId : parentTableIds) {
                if (parentList.length() > 0) {
                    parentList.append(",");
                }
                parentList.append(toRegclassLiteral(parentId));
            }
            query =
                    String.format(
                            "SELECT inhrelid::regclass AS child, inhparent::regclass AS parent "
                                    + "FROM pg_inherits WHERE inhparent IN (%s)",
                            parentList.toString());
        } else {
            query =
                    "SELECT inhrelid::regclass AS child, inhparent::regclass AS parent FROM pg_inherits";
        }

        LOG.debug("Executing partition discovery query: {}", query);

        try (Statement stmt = jdbc.connection().createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String childName = rs.getString("child");
                String parentName = rs.getString("parent");
                TableId childId = parseTableId(childName);
                TableId parentId = parseTableId(parentName);

                parentToChildren.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
                LOG.debug("Discovered partition: {} -> {}", parentId, childId);
            }
        }

        LOG.info(
                "Partition discovery complete: {} parents, {} total children",
                parentToChildren.size(),
                parentToChildren.values().stream().mapToInt(List::size).sum());
        return parentToChildren;
    }

    /**
     * Resolves parent→child mappings for specific child tables by querying pg_inherits via
     * inhrelid. This is O(unknown_count) instead of O(total_children), making it efficient for
     * incremental reconciliation when only a few new partitions are detected.
     *
     * @param childTableIds the specific child TableIds to resolve parents for
     * @param jdbc the JDBC connection
     * @return a map of parent TableId to list of child TableIds (only for the specified children)
     */
    public static Map<TableId, List<TableId>> resolveChildParents(
            Collection<TableId> childTableIds, JdbcConnection jdbc) throws SQLException {
        Map<TableId, List<TableId>> result = new HashMap<>();
        if (childTableIds == null || childTableIds.isEmpty()) {
            return result;
        }

        StringBuilder sql =
                new StringBuilder(
                        "SELECT inhrelid::regclass AS child, "
                                + "inhparent::regclass AS parent "
                                + "FROM pg_inherits WHERE inhrelid IN (");
        boolean first = true;
        for (TableId childId : childTableIds) {
            if (!first) {
                sql.append(',');
            }
            sql.append(toRegclassLiteral(childId));
            first = false;
        }
        sql.append(')');

        LOG.debug("Executing incremental partition resolution: {}", sql);

        try (Statement stmt = jdbc.connection().createStatement();
                ResultSet rs = stmt.executeQuery(sql.toString())) {
            while (rs.next()) {
                String childName = rs.getString("child");
                String parentName = rs.getString("parent");
                TableId childId = parseTableId(childName);
                TableId parentId = parseTableId(parentName);
                result.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
                LOG.debug("Resolved incremental partition: {} -> {}", parentId, childId);
            }
        }

        LOG.info(
                "Incremental partition resolution complete: {} new children found",
                result.values().stream().mapToInt(List::size).sum());
        return result;
    }

    /**
     * Builds a child→parent reverse mapping from a parent→children mapping.
     *
     * @param parentToChildren the parent→children mapping
     * @return a child→parent mapping
     */
    public static Map<TableId, TableId> buildChildToParentMapping(
            Map<TableId, List<TableId>> parentToChildren) {
        Map<TableId, TableId> childToParent = new HashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry : parentToChildren.entrySet()) {
            for (TableId childId : entry.getValue()) {
                childToParent.put(childId, entry.getKey());
            }
        }
        return childToParent;
    }

    /**
     * Extracts all child TableIds from a parent→children mapping.
     *
     * @param parentToChildren the parent→children mapping
     * @return a flat list of all child TableIds
     */
    public static List<TableId> getAllChildTableIds(Map<TableId, List<TableId>> parentToChildren) {
        List<TableId> allChildren = new ArrayList<>();
        for (List<TableId> children : parentToChildren.values()) {
            allChildren.addAll(children);
        }
        return allChildren;
    }

    /**
     * Parses a table identifier string (as returned by ::regclass) into a TableId.
     *
     * @param tableIdString the table identifier string (may be schema-qualified)
     * @return the parsed TableId
     */
    static TableId parseTableId(String tableIdString) {
        int dotIndex = tableIdString.lastIndexOf('.');
        if (dotIndex > 0) {
            String schema = stripOptionalQuotes(tableIdString.substring(0, dotIndex));
            String table = stripOptionalQuotes(tableIdString.substring(dotIndex + 1));
            return new TableId(null, schema, table);
        } else {
            return new TableId(null, "public", stripOptionalQuotes(tableIdString));
        }
    }

    /**
     * Converts a TableId into a safe regclass literal for use in SQL queries. Escapes both
     * double-quotes (for identifier quoting) and single-quotes (for SQL string literal context) to
     * prevent SQL injection.
     */
    private static String toRegclassLiteral(TableId tableId) {
        String qualifiedName;
        if (tableId.schema() != null) {
            qualifiedName =
                    String.format(
                            "%s.%s",
                            quoteIdentifier(tableId.schema()), quoteIdentifier(tableId.table()));
        } else {
            qualifiedName = quoteIdentifier(tableId.table());
        }
        // Escape single quotes for embedding within SQL string literal
        return "'" + qualifiedName.replace("'", "''") + "'::regclass";
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private static String stripOptionalQuotes(String identifier) {
        if (identifier.length() >= 2
                && identifier.charAt(0) == '"'
                && identifier.charAt(identifier.length() - 1) == '"') {
            return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
        }
        return identifier;
    }
}
