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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Catalog helper for discovering PostgreSQL declarative partition relationships. */
public final class PartitionCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCatalog.class);

    private PartitionCatalog() {}

    public static PartitionRoutingState discoverRoutingState(
            JdbcConnection jdbc, Collection<TableId> parentTableIds) throws SQLException {
        if (parentTableIds == null || parentTableIds.isEmpty()) {
            return PartitionRoutingState.EMPTY;
        }
        Map<TableId, List<TableId>> parentToChildren = discoverChildren(jdbc, parentTableIds);
        if (parentToChildren.isEmpty()) {
            return PartitionRoutingState.EMPTY;
        }
        LOG.info(
                "Discovered PostgreSQL partition routing: {} parents, {} children",
                parentToChildren.size(),
                parentToChildren.values().stream().mapToInt(List::size).sum());
        return PartitionRoutingState.of(parentToChildren);
    }

    public static Map<TableId, List<TableId>> discoverChildren(
            JdbcConnection jdbc, Collection<TableId> parentTableIds) throws SQLException {
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        if (parentTableIds == null || parentTableIds.isEmpty()) {
            return parentToChildren;
        }

        String parents =
                parentTableIds.stream()
                        .map(PartitionCatalog::toRegclassLiteral)
                        .collect(Collectors.joining(", "));
        String query =
                "SELECT "
                        + "child_ns.nspname AS child_schema, child.relname AS child_table, "
                        + "parent_ns.nspname AS parent_schema, parent.relname AS parent_table "
                        + "FROM pg_inherits "
                        + "JOIN pg_class child ON pg_inherits.inhrelid = child.oid "
                        + "JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid "
                        + "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid "
                        + "JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid "
                        + "WHERE pg_inherits.inhparent IN ("
                        + parents
                        + ") "
                        + "ORDER BY parent_ns.nspname, parent.relname, child_ns.nspname, child.relname";

        try (Statement statement = jdbc.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            while (rs.next()) {
                TableId parent =
                        new TableId(
                                null, rs.getString("parent_schema"), rs.getString("parent_table"));
                TableId child =
                        new TableId(
                                null, rs.getString("child_schema"), rs.getString("child_table"));
                parentToChildren.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(child);
            }
        }
        return parentToChildren;
    }

    public static TableId resolveParent(JdbcConnection jdbc, TableId childTableId)
            throws SQLException {
        String query =
                "SELECT parent_ns.nspname AS parent_schema, parent.relname AS parent_table "
                        + "FROM pg_inherits "
                        + "JOIN pg_class child ON pg_inherits.inhrelid = child.oid "
                        + "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid "
                        + "JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid "
                        + "WHERE pg_inherits.inhrelid = "
                        + toRegclassLiteral(childTableId)
                        + " LIMIT 1";
        try (Statement statement = jdbc.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            if (!rs.next()) {
                return null;
            }
            return new TableId(null, rs.getString("parent_schema"), rs.getString("parent_table"));
        }
    }

    public static List<TableId> expandParentsToChildren(
            Collection<TableId> capturedTables, PartitionRoutingState routingState) {
        List<TableId> expanded = new ArrayList<>();
        for (TableId tableId : capturedTables) {
            if (routingState.containsParent(tableId)) {
                expanded.addAll(routingState.childrenOf(tableId));
            } else {
                expanded.add(tableId);
            }
        }
        return expanded;
    }

    static String toRegclassLiteral(TableId tableId) {
        String qualifiedName =
                tableId.schema() == null
                        ? quoteIdentifier(tableId.table())
                        : quoteIdentifier(tableId.schema())
                                + "."
                                + quoteIdentifier(tableId.table());
        return "'" + qualifiedName.replace("'", "''") + "'::regclass";
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
