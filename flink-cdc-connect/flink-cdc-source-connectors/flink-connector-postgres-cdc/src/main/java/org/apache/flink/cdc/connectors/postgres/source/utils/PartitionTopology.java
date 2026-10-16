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

import javax.annotation.Nullable;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * The partition trees of the database at one position of the replication stream, kept up to date by
 * the rows of the partition marker table read from that stream.
 *
 * <p>PostgreSQL 10 logical decoding emits no DDL, and the catalog that can be queried only tells
 * the partitions of today, not those at the position being decoded or replayed. The partition
 * marker table closes that gap: an event trigger installed as a CDC prerequisite writes, in the
 * transaction of every partition DDL, a row {@code (lsn, parent, children)} with the direct
 * partitions of the parent after the DDL ({@code children} is {@code NULL} when {@code parent} was
 * dropped). The marker table is published, so its rows arrive in commit order, exactly between the
 * changes before and after the DDL.
 *
 * <p>At the start of a stream the topology is built from the markers with {@code lsn} not after the
 * start position. A DDL that changes the root of a partition holds an ACCESS EXCLUSIVE lock on the
 * partition from before its marker is written until it commits, so every transaction writing that
 * partition commits either before the marker {@code lsn} or after the DDL; the comparison is
 * therefore exact even when the start position falls between the marker and the commit of the DDL.
 */
public class PartitionTopology {

    public static final String MARKER_SCHEMA = "public";
    public static final String MARKER_TABLE = "flink_cdc_partition_marker";

    private static final String MARKER_EXISTS_SQL =
            "SELECT to_regclass('" + MARKER_SCHEMA + "." + MARKER_TABLE + "') IS NOT NULL";

    private static final String MARKER_PUBLISHED_SQL =
            "SELECT 1 FROM pg_publication_tables WHERE pubname = ? AND schemaname = '"
                    + MARKER_SCHEMA
                    + "' AND tablename = '"
                    + MARKER_TABLE
                    + "'";

    private static final String MARKERS_SQL =
            "SELECT lsn, parent, children FROM "
                    + MARKER_SCHEMA
                    + "."
                    + MARKER_TABLE
                    + " ORDER BY lsn, id";

    /** The direct partitions of every partitioned table, legacy INHERITS children excluded. */
    private static final String PARTITION_EDGES_SQL =
            "SELECT pn.nspname || '.' || pc.relname, cn.nspname || '.' || cc.relname"
                    + " FROM pg_inherits i"
                    + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                    + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                    + " JOIN pg_class pc ON pc.oid = i.inhparent"
                    + " JOIN pg_namespace pn ON pn.oid = pc.relnamespace"
                    + " WHERE cc.relispartition ORDER BY 1, 2";

    /** One row of the partition marker table. */
    public static final class Marker {
        final long lsn;
        final String parent;
        @Nullable final List<String> children;

        public Marker(long lsn, String parent, @Nullable List<String> children) {
            this.lsn = lsn;
            this.parent = parent;
            this.children = children;
        }

        @Override
        public String toString() {
            return "Marker{lsn=" + lsn + ", parent=" + parent + ", children=" + children + '}';
        }
    }

    private final String database;
    private final List<String> configuredTables;

    /** Partitioned table as {@code schema.table} to its direct partitions. */
    private final Map<String, List<String>> childrenByParent = new LinkedHashMap<>();

    /** The {@code lsn} of the last marker applied per parent, markers not after it are stale. */
    private final Map<String, Long> lsnByParent = new HashMap<>();

    /** Every partition routed at some point of this topology's lifetime. */
    private final Set<String> everRouted = new HashSet<>();

    private PostgresPartitionRouting routing = PostgresPartitionRouting.empty();

    private PartitionTopology(String database, List<String> configuredTables) {
        this.database = database;
        this.configuredTables = configuredTables;
    }

    public static boolean isMarkerTable(TableId tableId) {
        return MARKER_SCHEMA.equals(tableId.schema()) && MARKER_TABLE.equals(tableId.table());
    }

    /**
     * Builds the topology at the given stream position from the catalog and the marker table.
     *
     * @param position the larger of the {@code lsn} and {@code lsn_commit} of the start offset
     * @param includedPartitions the partitions in Debezium's include list: each is a partition at
     *     some position, so it is dropped wherever it is not routed, never captured as a table
     */
    public static PartitionTopology atPosition(
            Connection connection,
            long position,
            String database,
            List<String> configuredTables,
            Set<String> includedPartitions)
            throws SQLException {
        Map<String, List<String>> catalog = readEdges(connection);
        List<Marker> markers = readMarkers(connection).orElse(Collections.emptyList());
        if (!connection.getAutoCommit()) {
            // Never leave the shared connection idle in transaction: it would hold back vacuum.
            connection.commit();
        }
        PartitionTopology topology =
                atPosition(catalog, markers, position, database, configuredTables);
        topology.everRouted.addAll(includedPartitions);
        return topology;
    }

    static PartitionTopology atPosition(
            Map<String, List<String>> catalog,
            List<Marker> markers,
            long position,
            String database,
            List<String> configuredTables) {
        PartitionTopology topology = new PartitionTopology(database, configuredTables);
        if (markers.isEmpty() || position < markers.get(0).lsn) {
            // The position predates the marker table, the catalog is the best knowledge there is.
            catalog.forEach(
                    (parent, children) ->
                            topology.childrenByParent.put(parent, new ArrayList<>(children)));
        } else {
            for (Marker marker : markers) {
                if (marker.lsn > position) {
                    break;
                }
                topology.put(marker);
            }
        }
        topology.refresh();
        return topology;
    }

    /**
     * Returns every partition routed to a configured root by the catalog or at any marker, they all
     * have to be in Debezium's include list for the stream to see them.
     */
    static Set<String> partitionsEverRouted(
            Map<String, List<String>> catalog,
            List<Marker> markers,
            String database,
            List<String> configuredTables) {
        PartitionTopology topology =
                atPosition(catalog, Collections.emptyList(), 0, database, configuredTables);
        PartitionTopology history = new PartitionTopology(database, configuredTables);
        for (Marker marker : markers) {
            history.put(marker);
            history.refresh();
        }
        Set<String> partitions = new TreeSet<>(topology.everRouted);
        partitions.addAll(history.everRouted);
        return partitions;
    }

    /**
     * Applies a marker read from the stream.
     *
     * @return whether the routing changed
     */
    public boolean apply(Marker marker) {
        Long applied = lsnByParent.get(marker.parent);
        if (applied != null && marker.lsn <= applied) {
            // Already part of the topology the stream started with.
            return false;
        }
        put(marker);
        return refresh();
    }

    /** The routing at the current position. */
    public PostgresPartitionRouting routing() {
        return routing;
    }

    /** Returns whether the table is a partition that is not routed at the current position. */
    public boolean isUnrouted(TableId tableId) {
        String table = schemaDotTable(tableId);
        return everRouted.contains(table) && !routing.parentOf(table).isPresent();
    }

    /**
     * Checks a table filtered out by Debezium: a routed partition that is not in the include list
     * appeared after the source config was created and needs a restart to be included.
     */
    public void checkFiltered(TableId tableId) {
        Optional<String> root = routing.parentOf(schemaDotTable(tableId));
        if (root.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Partition %s of partition root %s is not in the include list of this "
                                    + "source; failing so that the restart includes it.",
                            schemaDotTable(tableId), root.get()));
        }
    }

    private void put(Marker marker) {
        lsnByParent.put(marker.parent, marker.lsn);
        if (marker.children == null) {
            // Dropped: neither a parent nor a partition any more.
            childrenByParent.remove(marker.parent);
            for (List<String> children : childrenByParent.values()) {
                children.remove(marker.parent);
            }
            return;
        }
        // A partition has one parent: a moved or renamed partition leaves its former parent.
        for (List<String> children : childrenByParent.values()) {
            children.removeAll(marker.children);
        }
        childrenByParent.put(marker.parent, new ArrayList<>(marker.children));
    }

    private boolean refresh() {
        Map<String, String> parentOf = new HashMap<>();
        childrenByParent.forEach(
                (parent, children) -> children.forEach(child -> parentOf.put(child, parent)));
        Map<String, Set<String>> ancestorsByChild = new LinkedHashMap<>();
        for (String child : parentOf.keySet()) {
            Set<String> ancestors = new TreeSet<>();
            for (String parent = parentOf.get(child);
                    parent != null && ancestors.add(parent);
                    parent = parentOf.get(parent)) {}
            ancestorsByChild.put(child, ancestors);
        }
        PostgresPartitionRouting refreshed =
                PostgresPartitionRouting.create(ancestorsByChild, database, configuredTables);
        boolean changed = !refreshed.sameRoutes(routing);
        routing = refreshed;
        everRouted.addAll(refreshed.routedPartitions());
        return changed;
    }

    /**
     * Checks that the installed marker table is in the publication.
     *
     * @throws IllegalStateException if it is not: its rows would never reach the stream and the
     *     routing would silently stop following partition DDL
     */
    static void checkPublished(Connection connection, String publication) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MARKER_PUBLISHED_SQL)) {
            statement.setString(1, publication);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException(
                            String.format(
                                    "The partition marker table %s.%s is not in publication %s; "
                                            + "add it (ALTER PUBLICATION %s ADD TABLE %s.%s) or "
                                            + "the partition routing cannot follow partition DDL.",
                                    MARKER_SCHEMA,
                                    MARKER_TABLE,
                                    publication,
                                    publication,
                                    MARKER_SCHEMA,
                                    MARKER_TABLE));
                }
            }
        }
    }

    /**
     * Returns the markers in {@code lsn} order, or empty when the marker table is not installed.
     */
    static Optional<List<Marker>> readMarkers(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(MARKER_EXISTS_SQL)) {
                if (!resultSet.next() || !resultSet.getBoolean(1)) {
                    return Optional.empty();
                }
            }
            List<Marker> markers = new ArrayList<>();
            try (ResultSet resultSet = statement.executeQuery(MARKERS_SQL)) {
                while (resultSet.next()) {
                    Array children = resultSet.getArray(3);
                    markers.add(
                            new Marker(
                                    resultSet.getLong(1),
                                    resultSet.getString(2),
                                    children == null
                                            ? null
                                            : Arrays.asList((String[]) children.getArray())));
                }
            }
            return Optional.of(markers);
        }
    }

    static Map<String, List<String>> readEdges(Connection connection) throws SQLException {
        Map<String, List<String>> childrenByParent = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(PARTITION_EDGES_SQL)) {
            while (resultSet.next()) {
                childrenByParent
                        .computeIfAbsent(resultSet.getString(1), parent -> new ArrayList<>())
                        .add(resultSet.getString(2));
            }
        }
        return childrenByParent;
    }

    private static String schemaDotTable(TableId tableId) {
        return tableId.schema() + "." + tableId.table();
    }
}
