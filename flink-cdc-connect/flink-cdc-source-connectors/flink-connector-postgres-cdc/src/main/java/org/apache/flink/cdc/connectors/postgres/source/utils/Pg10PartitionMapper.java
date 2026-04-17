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
 * Utility class for mapping parent↔child partitions and validating publication membership on PG10.
 *
 * <p>PG10 does not support PRIMARY KEY on partitioned tables, so parent→child partition routing
 * must be discovered via the pg_inherits catalog. This class provides utilities to:
 *
 * <ul>
 *   <li>Discover parent→child relationships from pg_inherits
 *   <li>Build child→parent reverse mapping
 *   <li>Validate that all child partitions are members of the configured publication
 * </ul>
 */
public class Pg10PartitionMapper {

    private static final Logger LOG = LoggerFactory.getLogger(Pg10PartitionMapper.class);

    /**
     * Discovers parent→child partition mappings by querying pg_inherits for the given parent
     * tables.
     *
     * @param parentTableIds the list of parent TableIds to query
     * @param jdbc the JDBC connection
     * @param filterByParent if true, only returns children for the specified parent tables; if
     *     false, returns all partition relationships
     * @return a map of parent TableId to list of child TableIds
     */
    public static Map<TableId, List<TableId>> discoverPartitionedTableMappings(
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
                String tableName =
                        parentId.schema() != null
                                ? parentId.schema() + "." + parentId.table()
                                : parentId.table();
                parentList.append("'").append(tableName).append("'::regclass");
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
                LOG.debug("Discovered partition relationship: {} -> {}", parentId, childId);
            }
        }

        LOG.info(
                "Discovered {} parent→child partition relationships",
                parentToChildren.values().stream().mapToInt(List::size).sum());
        return parentToChildren;
    }

    /**
     * Builds a child→parent mapping from a parent→children mapping.
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
     * @return a list of all child TableIds
     */
    public static List<TableId> getAllChildTableIds(Map<TableId, List<TableId>> parentToChildren) {
        List<TableId> allChildren = new ArrayList<>();
        for (List<TableId> children : parentToChildren.values()) {
            allChildren.addAll(children);
        }
        return allChildren;
    }

    /**
     * Validates that all child partitions are members of the specified publication.
     *
     * @deprecated Use {@link Pg10PublicationManager#validatePublicationMembership} directly.
     */
    @Deprecated
    public static void validatePublicationMembership(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        Pg10PublicationManager.validatePublicationMembership(jdbc, publicationName, childTableIds);
    }

    /**
     * Identifies child partitions that are NOT in the publication.
     *
     * @deprecated Use {@link Pg10PublicationManager#findMissingPublicationChildren} directly.
     */
    @Deprecated
    public static List<TableId> findChildrenMissingFromPublication(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        return Pg10PublicationManager.findMissingPublicationChildren(
                jdbc, publicationName, childTableIds);
    }

    /**
     * Adds child partitions to the publication at runtime.
     *
     * @deprecated Use {@link Pg10PublicationManager#addTablesToPublication} directly.
     */
    @Deprecated
    public static void addChildrenToPublication(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTableIds)
            throws SQLException {
        Pg10PublicationManager.addTablesToPublication(jdbc, publicationName, childTableIds);
    }

    /**
     * Parses a table identifier string into a TableId.
     *
     * @param tableIdString the table identifier string (may be schema.qualified)
     * @return the parsed TableId
     */
    private static TableId parseTableId(String tableIdString) {
        int dotIndex = tableIdString.lastIndexOf('.');
        if (dotIndex > 0) {
            String schema = tableIdString.substring(0, dotIndex);
            String table = tableIdString.substring(dotIndex + 1);
            return new TableId(null, schema, table);
        } else {
            return new TableId(null, "public", tableIdString);
        }
    }

    /** Exception thrown when partition publication validation fails. */
    public static class PartitionPublicationValidationException extends SQLException {
        private static final long serialVersionUID = 1L;

        public PartitionPublicationValidationException(String message) {
            super(message);
        }
    }
}
