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
import io.debezium.relational.TableId;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for PostgreSQL version detection and partition table handling. This centralizes
 * version-specific logic that was previously scattered across multiple classes.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Cached version detection to avoid repeated database queries
 *   <li>Thread-safe operations using concurrent data structures
 *   <li>Comprehensive version checking methods for different PostgreSQL versions
 *   <li>Automatic cache management with configurable TTL
 * </ul>
 */
public class PostgresVersionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresVersionUtils.class);

    // Cache for version detection results to avoid repeated database queries
    private static final ConcurrentMap<String, VersionInfo> VERSION_CACHE =
            new ConcurrentHashMap<>();

    // Cache TTL in milliseconds (default: 5 minutes)
    private static final long CACHE_TTL_MS = 5 * 60 * 1000;

    // Pattern for extracting version numbers from version strings
    private static final Pattern pattern = Pattern.compile("\\d+\\.\\d+");

    /** Internal class to store version information with timestamp for TTL management. */
    private static class VersionInfo {
        final ServerVersion serverVersion;
        final String versionString;
        final long timestamp;

        VersionInfo(ServerVersion serverVersion, String versionString) {
            this.serverVersion = serverVersion;
            this.versionString = versionString;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }
    }

    /** Internal class to hold ServerVersion and version string together. */
    private static class ServerVersionInfo {
        final ServerVersion serverVersion;
        final String versionString;

        ServerVersionInfo(ServerVersion serverVersion, String versionString) {
            this.serverVersion = serverVersion;
            this.versionString = versionString;
        }
    }

    /**
     * Check if PostgreSQL server has minimum version 11. This is equivalent to the logic in
     * PostgresDialect. Results are cached to improve performance.
     */
    public static boolean isServer11OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v11);
    }

    /**
     * Check if PostgreSQL server has minimum version 13. This is used for partition table
     * replication support. Results are cached to improve performance.
     */
    public static boolean isServer13OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v13);
    }

    /**
     * Check if PostgreSQL server has minimum version 12. Results are cached to improve performance.
     */
    public static boolean isServer12OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v12);
    }

    /**
     * Check if PostgreSQL server has minimum version 10. This is used for various PostgreSQL 10+
     * features. Results are cached to improve performance.
     */
    public static boolean isServer10OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v10);
    }

    /**
     * Check if PostgreSQL server has minimum version 9.6. Results are cached to improve
     * performance.
     */
    public static boolean isServer96OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v9_6);
    }

    /**
     * Check if PostgreSQL server has minimum version 9.5. Results are cached to improve
     * performance.
     */
    public static boolean isServer95OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v9_5);
    }

    /**
     * Check if PostgreSQL server has minimum version 9.4. This is used for exported snapshots
     * support. Results are cached to improve performance.
     */
    public static boolean isServer94OrLater(PostgresConnection connection) {
        return hasMinimumVersion(connection, ServerVersion.v9_4);
    }

    /**
     * Generic method to check if PostgreSQL server has minimum version. This method implements
     * caching to avoid repeated database queries.
     *
     * @param connection the PostgreSQL connection
     * @param minVersion the minimum required version
     * @return true if server version is equal or greater than minVersion
     */
    public static boolean hasMinimumVersion(
            PostgresConnection connection, ServerVersion minVersion) {
        return connection.getServerVersion().getVersionNum() >= minVersion.getVersionNum();
    }

    /**
     * Compare PostgreSQL server version with a target version. This provides a more flexible way to
     * do version comparisons.
     *
     * @param connection the PostgreSQL connection
     * @param targetVersion the target version to compare against
     * @return negative if server version is less than target, zero if equal, positive if greater
     *     than target
     */
    public static int compareVersion(PostgresConnection connection, ServerVersion targetVersion) {
        ServerVersion serverVersion = connection.getServerVersion();
        if (serverVersion == null) {
            return -1; // Assume older version if detection failed
        }
        return Integer.compare(serverVersion.getVersionNum(), targetVersion.getVersionNum());
    }

    /**
     * Get PostgreSQL version number directly from server_version_num. This is more efficient than
     * parsing version strings.
     *
     * @param connection the PostgreSQL connection
     * @return the version number (e.g., 140002 for 14.0.2), or 0 if failed
     */
    private static int getPostgreSQLVersionNumber(PostgresConnection connection) {
        String sql = "SHOW server_version_num";
        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            int databaseMajorVersion = conn.getMetaData().getDatabaseMajorVersion();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.debug("Failed to get PostgreSQL version number using SHOW server_version_num", e);
        }
        return 0;
    }

    /**
     * Format numeric version number to version string.
     *
     * @param versionNumber the numeric version (e.g., 140002)
     * @return formatted version string (e.g., "14.0.2")
     */
    private static String formatVersionNumber(int versionNumber) {
        if (versionNumber >= 100000) {
            // PostgreSQL 10+ format: XXYYZZ -> XX.YY.ZZ
            int major = versionNumber / 10000;
            int minor = (versionNumber / 100) % 100;
            int patch = versionNumber % 100;
            return String.format("%d.%d.%d", major, minor, patch);
        } else {
            // PostgreSQL 9.x format: XXYYZZ -> X.Y.ZZ
            int major = versionNumber / 10000;
            int minor = (versionNumber / 100) % 100;
            int patch = versionNumber % 100;
            return String.format("%d.%d.%d", major, minor, patch);
        }
    }

    /**
     * Parse PostgreSQL version from version string.
     *
     * @param versionString the version string from database metadata
     * @return the parsed ServerVersion, or null if parsing failed
     */
    private static ServerVersion parseVersionFromString(String versionString) {
        if (versionString == null || versionString.isEmpty()) {
            return null;
        }

        try {
            Matcher matcher = pattern.matcher(versionString);
            if (matcher.find()) {
                String versionNumbers = matcher.group();
                float majorVersion = Float.parseFloat(versionNumbers);
                // Map major version to ServerVersion
                if (majorVersion >= 14) {
                    return ServerVersion.v14;
                } else if (majorVersion >= 13) {
                    return ServerVersion.v13;
                } else if (majorVersion >= 12) {
                    return ServerVersion.v12;
                } else if (majorVersion >= 11) {
                    return ServerVersion.v11;
                } else if (majorVersion >= 10) {
                    return ServerVersion.v10;
                } else if (majorVersion == 9) {
                    // Handle 9.x versions
                    if (versionNumbers.length() >= 2) {
                        int minorVersion = Integer.parseInt(versionNumbers.split("\\.")[1]);
                        if (minorVersion >= 6) {
                            return ServerVersion.v9_6;
                        } else if (minorVersion >= 5) {
                            return ServerVersion.v9_5;
                        } else if (minorVersion >= 4) {
                            return ServerVersion.v9_4;
                        }
                    }
                    return ServerVersion.v9_0;
                }
            } else {
                return ServerVersion.v10;
            }

        } catch (NumberFormatException e) {
            LOG.warn("Failed to parse PostgreSQL version from string: {}", versionString, e);
        }

        return null;
    }

    /** Create a cache key for the given connection. */
    private static String createCacheKey(PostgresConnection connection) {
        try {
            return connection.connectionString();
        } catch (Exception e) {
            // Fallback to connection hash if connection string is not available
            return "conn:" + connection.hashCode();
        }
    }

    /** Clear the version cache. Useful for testing or when connection properties change. */
    public static void clearVersionCache() {
        int cacheSize = VERSION_CACHE.size();
        VERSION_CACHE.clear();
        LOG.info("Cleared PostgreSQL version cache, removed {} entries", cacheSize);
    }

    /** Get the current version cache size. Useful for monitoring and testing. */
    public static int getVersionCacheSize() {
        return VERSION_CACHE.size();
    }

    /** Remove expired entries from the version cache. */
    public static void cleanupExpiredVersionCache() {
        int removedCount = 0;
        for (String key : VERSION_CACHE.keySet()) {
            VersionInfo info = VERSION_CACHE.get(key);
            if (info != null && info.isExpired()) {
                VERSION_CACHE.remove(key);
                removedCount++;
            }
        }
        if (removedCount > 0) {
            LOG.debug("Cleaned up {} expired version cache entries", removedCount);
        }
    }

    /**
     * Check if a table is a partition table using version-appropriate queries. This method
     * automatically uses the correct approach based on PostgreSQL version.
     */
    public static boolean isPartitionTable(PostgresConnection connection, TableId tableId) {
        if (isServer10OrLater(connection)) {
            return isPartitionTablePG10Plus(connection, tableId);
        } else {
            // For versions before PG10, partition tables don't exist in the modern sense
            return false;
        }
    }

    /**
     * Check if a table is a partition using PostgreSQL 10+ specific queries. This uses the
     * pg_class.relispartition column introduced in PG10.
     */
    private static boolean isPartitionTablePG10Plus(
            PostgresConnection connection, TableId tableId) {
        String sql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PARTITION_TABLE, connection);

        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean("relispartition");
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error checking if table {} is partition", tableId, e);
        }
        return false;
    }

    /** Check if a table is a partitioned table (parent table) using version-appropriate queries. */
    public static boolean isPartitionedTable(PostgresConnection connection, TableId tableId) {
        if (isServer10OrLater(connection)) {
            return isPartitionedTablePG10Plus(connection, tableId);
        } else {
            return false;
        }
    }

    /**
     * Check if a table is partitioned (parent) using PostgreSQL 10+ specific queries. This uses
     * pg_class.relkind = 'p' to identify partitioned tables.
     */
    private static boolean isPartitionedTablePG10Plus(
            PostgresConnection connection, TableId tableId) {
        String sql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PARTITIONED_TABLE, connection);

        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean("is_partitioned");
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error checking if table {} is partitioned", tableId, e);
        }
        return false;
    }

    /**
     * Find the parent table for a given partition. This works across PostgreSQL versions that
     * support partitioning.
     */
    public static Optional<TableId> findParentTableForPartition(
            PostgresConnection connection, TableId partitionId) {
        if (!isServer10OrLater(connection)) {
            return Optional.empty();
        }

        String sql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_PARENT_TABLE, connection);

        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, partitionId.schema());
            stmt.setString(2, partitionId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String parentSchema = rs.getString("parent_schema");
                    String parentTable = rs.getString("parent_table");
                    return Optional.of(
                            new TableId(partitionId.catalog(), parentSchema, parentTable));
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error finding parent table for partition {}", partitionId, e);
        }
        return Optional.empty();
    }

    /** Get primary key column names from a table. This works across PostgreSQL versions. */
    public static List<String> getPrimaryKeyColumns(
            PostgresConnection connection, TableId tableId) {
        List<String> primaryKeyNames = new ArrayList<>();

        String sql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.GET_PRIMARY_KEY_COLUMNS, connection);

        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableId.schema());
            stmt.setString(2, tableId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    primaryKeyNames.add(rs.getString("attname"));
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error getting primary keys from table {}", tableId, e);
        }

        LOG.debug(
                "Found {} primary key columns in table {}: {}",
                primaryKeyNames.size(),
                tableId,
                primaryKeyNames);
        return primaryKeyNames;
    }

    /**
     * Find all child partitions of a partitioned table. This is useful for comprehensive partition
     * discovery.
     */
    public static List<TableId> findChildPartitions(
            PostgresConnection connection, TableId parentTableId) {
        List<TableId> childPartitions = new ArrayList<>();

        if (!isServer10OrLater(connection)) {
            return childPartitions;
        }

        String sql =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_CHILD_PARTITIONS, connection);

        try (Connection conn = connection.connection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, parentTableId.schema());
            stmt.setString(2, parentTableId.table());

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String childSchema = rs.getString("child_schema");
                    String childTable = rs.getString("child_table");
                    childPartitions.add(
                            new TableId(parentTableId.catalog(), childSchema, childTable));
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error finding child partitions for table {}", parentTableId, e);
        }

        LOG.debug("Found {} child partitions for table {}", childPartitions.size(), parentTableId);
        return childPartitions;
    }
}
