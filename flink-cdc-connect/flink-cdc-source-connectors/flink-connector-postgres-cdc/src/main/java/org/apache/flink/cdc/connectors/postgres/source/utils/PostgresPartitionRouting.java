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
 * table.include.list} and are captured at all, and maps the identity of a captured partition back to
 * its root, so that one table identity and one {@code CreateTableEvent} is produced per root no
 * matter how many partitions exist.
 *
 * <p>Table names are handled as {@code schema.table}, which is how Debezium maps a PostgreSQL {@code
 * TableId} for {@code table.include.list}. {@link #empty()} is returned when the server supports
 * {@code publish_via_partition_root} (PostgreSQL 13+), when the feature is switched off, or when the
 * metadata query fails, in which case the connector behaves exactly as without this feature.
 */
public class PostgresPartitionRouting implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionRouting.class);

    /** The first PostgreSQL version with {@code publish_via_partition_root}. */
    public static final int PUBLISH_VIA_PARTITION_ROOT_SINCE = 130000;

    private static final PostgresPartitionRouting EMPTY =
            new PostgresPartitionRouting(Collections.emptyMap());

    private static final String PARTITION_ANCESTORS_SQL =
            "WITH RECURSIVE partition_tree(child_oid, ancestor_oid) AS ("
                    + " SELECT inhrelid, inhparent FROM pg_inherits"
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

    /** Child partition as {@code schema.table} to the configured partition root of that child. */
    private final Map<String, String> parentByChild;

    /** Partition root as {@code schema.table} to its child partitions as {@code schema.table}. */
    private final Map<String, List<String>> childrenByParent;

    private PostgresPartitionRouting(Map<String, String> parentByChild) {
        this.parentByChild = parentByChild;
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
     * Resolves the partition routing from the database. Never fails: whenever the server does not
     * need this routing or the metadata query fails, an {@link #empty()} routing is returned.
     *
     * @param configuredTables the configured table list, entries are regular expressions
     */
    public static PostgresPartitionRouting resolve(
            String jdbcUrl,
            String username,
            String password,
            List<String> configuredTables,
            boolean includePartitionedTables) {
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
            PostgresPartitionRouting routing =
                    create(
                            readPartitionAncestors(connection),
                            databaseOf(jdbcUrl),
                            configuredTables);
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
            LOG.warn(
                    "Failed to resolve the partition routing of {}, capture the configured tables "
                            + "as they are.",
                    jdbcUrl,
                    e);
            return empty();
        }
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
        return driver.connect(jdbcUrl, properties);
    }

    /** Builds a routing from the ancestor relations returned by {@link #PARTITION_ANCESTORS_SQL}. */
    static PostgresPartitionRouting create(
            Map<String, Set<String>> ancestorsByChild,
            String database,
            List<String> configuredTables) {
        Map<String, String> parentByChild = new LinkedHashMap<>();
        for (String child : new TreeSet<>(ancestorsByChild.keySet())) {
            String partitionRoot =
                    partitionRootOf(ancestorsByChild.get(child), ancestorsByChild.keySet());
            if (partitionRoot != null
                    && matchesConfiguredTable(partitionRoot, database, configuredTables)) {
                parentByChild.put(child, partitionRoot);
            }
        }
        return parentByChild.isEmpty() ? empty() : new PostgresPartitionRouting(parentByChild);
    }

    /** Returns whether no child partition is routed to a configured partition root. */
    public boolean isEmpty() {
        return parentByChild.isEmpty();
    }

    /** Returns the configured partition root of the given {@code schema.table}, if any. */
    public Optional<String> parentOf(String schemaDotTable) {
        return Optional.ofNullable(parentByChild.get(schemaDotTable));
    }

    /**
     * Returns one child partition of the given partition root as {@code schema.table}. A PostgreSQL
     * 10 partition root carries no primary key of its own, the table identity has to be taken from a
     * partition.
     */
    public Optional<String> sampleChildOf(String schemaDotTable) {
        List<String> children = childrenByParent.get(schemaDotTable);
        return children == null || children.isEmpty()
                ? Optional.empty()
                : Optional.of(children.get(0));
    }

    /** Returns the child partitions of the given partition root as {@code schema.table}. */
    public List<String> childrenOf(String schemaDotTable) {
        return childrenByParent.getOrDefault(schemaDotTable, Collections.emptyList());
    }

    /**
     * Returns the child partitions of the configured partition roots as {@code schema.table}, ready
     * to be appended to the configured table list for Debezium's {@code table.include.list}.
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
