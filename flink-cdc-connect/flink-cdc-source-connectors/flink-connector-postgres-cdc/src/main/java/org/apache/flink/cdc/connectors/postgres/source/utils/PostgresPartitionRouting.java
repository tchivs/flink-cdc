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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Routing from the child partitions of a partitioned table back to its partition root, for servers
 * without {@code publish_via_partition_root} (PostgreSQL 10/11/12).
 *
 * <p>Such servers can never publish a partitioned parent, pgoutput always reports the identity of
 * the leaf partition. When only the partition root is configured, this class resolves the leaf
 * partitions of the configured roots so that they can be added to Debezium's {@code
 * table.include.list} and are captured at all, and maps the identity of a captured partition back
 * to its root, so that one table identity and one {@code CreateTableEvent} is produced per root no
 * matter how many partitions exist.
 *
 * <p>Table names are handled as {@code schema.table}, which is how Debezium maps a PostgreSQL
 * {@code TableId} for {@code table.include.list}. {@link #empty()} is returned when the server
 * supports {@code publish_via_partition_root} (PostgreSQL 13+) or when the feature is switched off,
 * in which case the connector behaves exactly as without this feature.
 */
public class PostgresPartitionRouting implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionRouting.class);

    /** The first PostgreSQL version with {@code publish_via_partition_root}. */
    public static final int PUBLISH_VIA_PARTITION_ROOT_SINCE = 130000;

    private static final PostgresPartitionRouting EMPTY =
            new PostgresPartitionRouting(
                    Collections.emptyMap(), Collections.emptySet(), Collections.emptySet(), false);

    // relispartition: pg_inherits also holds legacy INHERITS children, whose parent stores rows of
    // its own and must neither be routed nor dropped from the snapshot. A partitioned table cannot
    // take part in legacy inheritance, so filtering the leaf level is enough.
    private static final String PARTITION_ANCESTORS_SQL =
            "WITH RECURSIVE partition_tree(child_oid, ancestor_oid) AS ("
                    + " SELECT i.inhrelid, i.inhparent FROM pg_inherits i"
                    + " JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relispartition"
                    + " UNION"
                    + " SELECT t.child_oid, i.inhparent FROM partition_tree t"
                    + " JOIN pg_inherits i ON i.inhrelid = t.ancestor_oid)"
                    + " SELECT child_ns.nspname || '.' || child_cl.relname,"
                    + " parent_ns.nspname || '.' || parent_cl.relname"
                    + " FROM partition_tree t"
                    + " JOIN pg_class child_cl ON child_cl.oid = t.child_oid"
                    + " JOIN pg_namespace child_ns ON child_ns.oid = child_cl.relnamespace"
                    + " JOIN pg_class parent_cl ON parent_cl.oid = t.ancestor_oid"
                    + " JOIN pg_namespace parent_ns ON parent_ns.oid = parent_cl.relnamespace"
                    + " ORDER BY 1, 2";

    /** The top level partition root of one partition, no row when it is not a partition. */
    private static final String PARTITION_ROOT_SQL =
            "WITH RECURSIVE ancestors(oid, depth) AS ("
                    + " SELECT i.inhparent, 1 FROM pg_inherits i"
                    + " JOIN pg_class c ON c.oid = i.inhrelid"
                    + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    + " WHERE c.relispartition AND n.nspname = ? AND c.relname = ?"
                    + " UNION"
                    + " SELECT i.inhparent, a.depth + 1 FROM ancestors a"
                    + " JOIN pg_inherits i ON i.inhrelid = a.oid)"
                    + " SELECT n.nspname || '.' || c.relname FROM ancestors a"
                    + " JOIN pg_class c ON c.oid = a.oid"
                    + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    + " ORDER BY a.depth DESC LIMIT 1";

    /** Child partition as {@code schema.table} to the configured partition root of that child. */
    private final Map<String, String> parentByChild;

    /** Partition root as {@code schema.table} to its child partitions as {@code schema.table}. */
    private final Map<String, List<String>> childrenByParent;

    /**
     * Every {@code schema.table} that is an ancestor of a routed child partition, the configured
     * roots and the intermediate levels of a multi level tree included. Such a table holds no rows
     * of its own: its data lives in the leaf partitions.
     */
    private final Set<String> partitionAncestors;

    /**
     * Partitions routed at some marker but not routed now: they stay in Debezium's include list so
     * that a replay still sees their changes from before a DETACH, but they are not snapshotted.
     */
    private final Set<String> formerPartitions;

    /**
     * Whether the partition marker table is installed, so that the stream follows partition DDL by
     * position through {@link PartitionTopology}.
     */
    private final boolean markers;

    private PostgresPartitionRouting(
            Map<String, String> parentByChild,
            Set<String> partitionAncestors,
            Set<String> formerPartitions,
            boolean markers) {
        this.parentByChild = parentByChild;
        this.partitionAncestors = partitionAncestors;
        this.formerPartitions = formerPartitions;
        this.markers = markers;
        Map<String, List<String>> childrenByParent = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : parentByChild.entrySet()) {
            childrenByParent
                    .computeIfAbsent(entry.getValue(), parent -> new ArrayList<>())
                    .add(entry.getKey());
        }
        this.childrenByParent = childrenByParent;
    }

    /** A routing that maps nothing, i.e. the connector behaves as without this feature. */
    public static PostgresPartitionRouting empty() {
        return EMPTY;
    }

    /**
     * Resolves the partition routing from the database. An {@link #empty()} routing is returned
     * when the feature is switched off or the server does not need this routing.
     *
     * @param configuredTables the configured table list, entries are regular expressions
     * @param publication the publication of the replication slot, the partition marker table has to
     *     be in it when installed
     * @throws IllegalStateException if the catalog cannot be read: on PostgreSQL 10/11/12 a
     *     partition root never carries changes of its own, so running without the routing would
     *     capture none of its changes, silently; or if the partition marker table is installed but
     *     not published
     */
    public static PostgresPartitionRouting resolve(
            String jdbcUrl,
            String username,
            String password,
            List<String> configuredTables,
            boolean includePartitionedTables,
            String publication) {
        if (!includePartitionedTables || configuredTables == null || configuredTables.isEmpty()) {
            return empty();
        }
        try (Connection connection = openConnection(jdbcUrl, username, password)) {
            int serverVersion = readServerVersion(connection);
            if (serverVersion >= PUBLISH_VIA_PARTITION_ROOT_SINCE) {
                LOG.info(
                        "PostgreSQL server {} supports publish_via_partition_root, no client side "
                                + "partition routing is needed.",
                        serverVersion);
                return empty();
            }
            String database = databaseOf(jdbcUrl);
            Map<String, List<String>> edges = PartitionTopology.readEdges(connection);
            PostgresPartitionRouting routing =
                    create(readPartitionAncestors(connection), database, configuredTables);
            Optional<List<PartitionTopology.Marker>> markers =
                    PartitionTopology.readMarkers(connection);
            Set<String> former =
                    markers.isPresent()
                            ? PartitionTopology.partitionsEverRouted(
                                    edges, markers.get(), database, configuredTables)
                            : Collections.emptySet();
            if (!routing.isEmpty() || !former.isEmpty()) {
                if (markers.isPresent()) {
                    PartitionTopology.checkPublished(connection, publication);
                    former.removeAll(routing.parentByChild.keySet());
                    routing =
                            new PostgresPartitionRouting(
                                    routing.parentByChild,
                                    routing.partitionAncestors,
                                    former,
                                    true);
                    LOG.info(
                            "Partition marker table found, partition routing follows partition "
                                    + "DDL by stream position; former partitions kept in the "
                                    + "include list: {}",
                            former);
                } else {
                    LOG.warn(
                            "Partition marker table {}.{} is not installed: a restart between a "
                                    + "DETACH and the next checkpoint loses the changes of the "
                                    + "detached partition made since that checkpoint.",
                            PartitionTopology.MARKER_SCHEMA,
                            PartitionTopology.MARKER_TABLE);
                }
            }
            if (routing.isEmpty()) {
                LOG.info(
                        "None of the configured tables is a partition root, no client side "
                                + "partition routing is needed.");
            } else {
                LOG.info(
                        "Client side partition routing is enabled, partitions are mapped to their "
                                + "root table: {}",
                        routing.parentByChild);
            }
            return routing;
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to resolve the partition routing of %s; failing instead of "
                                    + "silently capturing none of the partitions of %s.",
                            jdbcUrl, configuredTables),
                    e);
        }
    }

    /**
     * Returns the configured partition root of one table, or empty when the table is not a
     * partition of a configured root. Unlike {@link #resolve}, failures are thrown: the caller must
     * not mistake an unreadable catalog for "not a partition".
     *
     * @param configuredTables the configured table list, entries are regular expressions
     */
    static Optional<String> configuredRootOf(
            Connection connection,
            String database,
            List<String> configuredTables,
            String schema,
            String table)
            throws SQLException {
        Optional<String> root = Optional.empty();
        try (PreparedStatement statement = connection.prepareStatement(PARTITION_ROOT_SQL)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()
                        && matchesConfiguredTable(
                                resultSet.getString(1), database, configuredTables)) {
                    root = Optional.of(resultSet.getString(1));
                }
            }
        }
        if (!connection.getAutoCommit()) {
            // Never leave the shared connection idle in transaction: it would hold back vacuum.
            connection.commit();
        }
        return root;
    }

    /**
     * Opens the metadata connection without {@link DriverManager}: inside a Flink task the JDBC
     * driver is loaded by a user class loader, so it is not registered with the system {@link
     * DriverManager} and {@code DriverManager.getConnection} would fail with "No suitable driver".
     */
    private static Connection openConnection(String jdbcUrl, String username, String password)
            throws Exception {
        Driver driver =
                (Driver)
                        Class.forName("org.postgresql.Driver")
                                .getDeclaredConstructor()
                                .newInstance();
        Properties properties = new Properties();
        properties.setProperty("user", username == null ? "" : username);
        properties.setProperty("password", password == null ? "" : password);
        Connection connection = driver.connect(jdbcUrl, properties);
        if (connection == null) {
            // Driver#connect returns null instead of failing for a url it does not accept.
            throw new SQLException("The PostgreSQL driver does not accept " + jdbcUrl);
        }
        return connection;
    }

    /**
     * Builds a routing from the ancestor relations returned by {@link #PARTITION_ANCESTORS_SQL}.
     */
    static PostgresPartitionRouting create(
            Map<String, Set<String>> ancestorsByChild,
            String database,
            List<String> configuredTables) {
        Map<String, String> parentByChild = new LinkedHashMap<>();
        Set<String> partitionAncestors = new TreeSet<>();
        for (String child : new TreeSet<>(ancestorsByChild.keySet())) {
            String partitionRoot =
                    partitionRootOf(ancestorsByChild.get(child), ancestorsByChild.keySet());
            if (partitionRoot != null
                    && matchesConfiguredTable(partitionRoot, database, configuredTables)) {
                parentByChild.put(child, partitionRoot);
                // The root and every intermediate level of the tree hold no rows of their own.
                partitionAncestors.addAll(ancestorsByChild.get(child));
            }
        }
        return parentByChild.isEmpty()
                ? empty()
                : new PostgresPartitionRouting(
                        parentByChild, partitionAncestors, Collections.emptySet(), false);
    }

    /** Returns whether no child partition is routed to a configured partition root. */
    public boolean isEmpty() {
        return parentByChild.isEmpty();
    }

    /**
     * Returns whether the partition marker table is installed, i.e. the stream has to follow
     * partition DDL by position through a {@link PartitionTopology}.
     */
    public boolean hasMarkers() {
        return markers;
    }

    /**
     * Returns whether the given {@code schema.table} was routed at some marker but is not routed
     * now: it is in the include list for the stream only and must not be snapshotted.
     */
    public boolean isFormerPartition(String schemaDotTable) {
        return formerPartitions.contains(schemaDotTable);
    }

    /** Returns the routed child partitions as {@code schema.table}. */
    public Set<String> routedPartitions() {
        return parentByChild.keySet();
    }

    /**
     * Returns every partition this routing puts into Debezium's include list, the routed and the
     * former ones: a stream must route each of them by position, never capture one as a table.
     */
    public Set<String> partitionsInIncludeList() {
        Set<String> partitions = new TreeSet<>(parentByChild.keySet());
        partitions.addAll(formerPartitions);
        return partitions;
    }

    /** Returns whether both route the same partitions to the same roots. */
    public boolean sameRoutes(PostgresPartitionRouting other) {
        return parentByChild.equals(other.parentByChild);
    }

    /**
     * Returns whether the given {@code schema.table} is a partition ancestor whose captured data
     * lives in its leaf partitions, so it should be excluded from snapshot discovery.
     *
     * <p>On a server without {@code publish_via_partition_root} the leaf partitions are the data
     * carriers; the configured root and every intermediate partition ancestor hold no rows of their
     * own and, on PostgreSQL 10, cannot even carry a primary key. Keeping them in the discovered
     * list would make the snapshot chunk splitter fail and would read every row a second time.
     */
    public boolean hasCapturedPartitions(String schemaDotTable) {
        return partitionAncestors.contains(schemaDotTable);
    }

    /** Returns the configured partition root of the given {@code schema.table}, if any. */
    public Optional<String> parentOf(String schemaDotTable) {
        return Optional.ofNullable(parentByChild.get(schemaDotTable));
    }

    /**
     * Returns one child partition of the given partition root as {@code schema.table}. A PostgreSQL
     * 10 partition root carries no primary key of its own, the table identity has to be taken from
     * a partition.
     */
    public Optional<String> sampleChildOf(String schemaDotTable) {
        List<String> children = childrenByParent.get(schemaDotTable);
        if (children == null || children.isEmpty()) {
            return Optional.empty();
        }
        for (String child : children) {
            if (!hasCapturedPartitions(child)) {
                // A leaf partition: an intermediate level holds no primary key to borrow.
                return Optional.of(child);
            }
        }
        return Optional.of(children.get(0));
    }

    /** Returns the child partitions of the given partition root as {@code schema.table}. */
    public List<String> childrenOf(String schemaDotTable) {
        return childrenByParent.getOrDefault(schemaDotTable, Collections.emptyList());
    }

    /**
     * Returns the child partitions of the configured partition roots as {@code schema.table}, ready
     * to be appended to the configured table list for Debezium's {@code table.include.list}. Former
     * partitions are included: a replay must still see their changes from before a DETACH.
     */
    public List<String> expansionForDebeziumIncludeList(
            String database, List<String> configuredTables) {
        List<String> expansion = new ArrayList<>();
        for (Map.Entry<String, String> entry : parentByChild.entrySet()) {
            String parent = entry.getValue();
            String child = entry.getKey();
            String databaseQualified =
                    database == null || database.isEmpty() ? null : database + "." + parent;
            String childQualified =
                    database == null || database.isEmpty() ? null : database + "." + child;
            boolean matchedPlain = false;
            boolean matchedDatabaseQualified = false;
            for (String configuredTable : configuredTables) {
                if (configuredTable == null || configuredTable.isEmpty()) {
                    continue;
                }
                if (!matchedPlain && Pattern.matches(configuredTable, parent)) {
                    matchedPlain = true;
                }
                if (!matchedDatabaseQualified
                        && databaseQualified != null
                        && Pattern.matches(configuredTable, databaseQualified)) {
                    matchedDatabaseQualified = true;
                }
            }
            // Emit both shapes whenever the parent matched in either shape: JDBC table discovery
            // reports schema.table while the replication connection matches database.schema.table,
            // and which shape the configured entry uses must not decide that.
            if (matchedPlain || matchedDatabaseQualified) {
                expansion.add(child);
                if (childQualified != null) {
                    expansion.add(childQualified);
                }
            }
        }
        for (String former : formerPartitions) {
            // Routed to a configured root at some marker, so the root matched the configuration.
            expansion.add(former);
            if (database != null && !database.isEmpty()) {
                expansion.add(database + "." + former);
            }
        }
        return expansion;
    }

    /**
     * Returns whether the given {@code schema.table} is matched by the configured table list, which
     * may name tables as {@code schema.table} or as {@code database.schema.table}.
     */
    private static boolean matchesConfiguredTable(
            String schemaDotTable, String database, List<String> configuredTables) {
        String databaseQualified =
                database == null || database.isEmpty() ? null : database + "." + schemaDotTable;
        for (String configuredTable : configuredTables) {
            if (configuredTable == null || configuredTable.isEmpty()) {
                continue;
            }
            if (Pattern.matches(configuredTable, schemaDotTable)
                    || (databaseQualified != null
                            && Pattern.matches(configuredTable, databaseQualified))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the top level partition root of a partition, or {@code null} if the ancestors are
     * inconsistent. Intermediate levels of a multi level partitioning tree are not routing targets,
     * the whole tree is reported as the root.
     */
    private static String partitionRootOf(Set<String> ancestors, Set<String> allChildren) {
        if (ancestors == null) {
            return null;
        }
        for (String ancestor : new TreeSet<>(ancestors)) {
            if (!allChildren.contains(ancestor)) {
                return ancestor;
            }
        }
        return null;
    }

    private static int readServerVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW server_version_num")) {
            // An unreadable version is treated as a version that needs no routing at all.
            return resultSet.next()
                    ? Integer.parseInt(resultSet.getString(1).trim())
                    : Integer.MAX_VALUE;
        }
    }

    private static Map<String, Set<String>> readPartitionAncestors(Connection connection)
            throws SQLException {
        Map<String, Set<String>> ancestorsByChild = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(PARTITION_ANCESTORS_SQL)) {
            while (resultSet.next()) {
                ancestorsByChild
                        .computeIfAbsent(resultSet.getString(1), child -> new TreeSet<>())
                        .add(resultSet.getString(2));
            }
        }
        return ancestorsByChild;
    }

    /** Extracts the database name of a {@code jdbc:postgresql://host:port/database} url. */
    static String databaseOf(String jdbcUrl) {
        int authorityEnd = jdbcUrl.indexOf("//");
        String urlWithoutScheme = authorityEnd < 0 ? jdbcUrl : jdbcUrl.substring(authorityEnd + 2);
        int queryStart = urlWithoutScheme.indexOf('?');
        String path = queryStart < 0 ? urlWithoutScheme : urlWithoutScheme.substring(0, queryStart);
        int pathStart = path.indexOf('/');
        return pathStart < 0 ? null : path.substring(pathStart + 1);
    }
}
