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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Centralized SQL query management for PostgreSQL CDC connector. This class eliminates SQL query
 * string duplication across different utility classes and organizes queries by PostgreSQL version
 * and functionality.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Version-specific SQL queries organized by PostgreSQL version
 *   <li>Centralized query management to eliminate duplication
 *   <li>Type-safe query access through enums
 *   <li>Automatic version-appropriate query selection
 *   <li>Comprehensive documentation for each query
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * String query = PostgresSqlQueries.getQuery(QueryType.CHECK_PARTITION_TABLE, connection);
 * // Use the query with prepared statement
 * }</pre>
 */
public class PostgresSqlQueries {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSqlQueries.class);

    /**
     * Enumeration of all SQL query types supported by the PostgreSQL CDC connector. Each query type
     * has version-specific implementations.
     */
    public enum QueryType {
        /** Check if a table is a partition table (child table). */
        CHECK_PARTITION_TABLE,

        /** Check if a table is a partitioned table (parent table). */
        CHECK_PARTITIONED_TABLE,

        /** Find the parent table for a given partition. */
        FIND_PARENT_TABLE,

        /** Get primary key columns for a table. */
        GET_PRIMARY_KEY_COLUMNS,

        /** Find all child partitions of a partitioned table. */
        FIND_CHILD_PARTITIONS,

        /** Check partition table with additional metadata (PG10 specific). */
        CHECK_PARTITION_TABLE_WITH_METADATA,

        /** Check if a publication exists by name. */
        CHECK_PUBLICATION_EXISTS,

        /** Find partitioned tables that match table filter conditions. */
        FIND_PARTITIONED_TABLES_BY_FILTER,

        /** Get all tables in a schema (for publication management). */
        GET_SCHEMA_TABLES,

        /** Find child partitions for a given partitioned table (alternative implementation). */
        FIND_CHILD_PARTITIONS_ALT
    }

    /**
     * PostgreSQL version-specific SQL queries. Each version strategy has its own set of optimized
     * queries.
     */
    private static final Map<PostgresVersionStrategy, Map<QueryType, String>> VERSION_QUERIES;

    static {
        Map<PostgresVersionStrategy, Map<QueryType, String>> versionQueries = new HashMap<>();

        // PostgreSQL 10 specific queries
        Map<QueryType, String> pg10Queries = new HashMap<>();
        pg10Queries.put(
                QueryType.CHECK_PARTITION_TABLE,
                "SELECT c.relispartition "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE n.nspname = ? AND c.relname = ?");

        pg10Queries.put(
                QueryType.CHECK_PARTITION_TABLE_WITH_METADATA,
                "SELECT c.relispartition, c.relkind "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE n.nspname = ? AND c.relname = ?");

        pg10Queries.put(
                QueryType.CHECK_PARTITIONED_TABLE,
                "SELECT c.relkind = 'p' as is_partitioned "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE n.nspname = ? AND c.relname = ?");

        pg10Queries.put(
                QueryType.FIND_PARENT_TABLE,
                "SELECT pn.nspname as parent_schema, pc.relname as parent_table "
                        + "FROM pg_inherits i "
                        + "JOIN pg_class pc ON i.inhparent = pc.oid "
                        + "JOIN pg_namespace pn ON pc.relnamespace = pn.oid "
                        + "JOIN pg_class cc ON i.inhrelid = cc.oid "
                        + "JOIN pg_namespace cn ON cc.relnamespace = cn.oid "
                        + "WHERE cn.nspname = ? AND cc.relname = ?");

        pg10Queries.put(
                QueryType.GET_PRIMARY_KEY_COLUMNS,
                "SELECT a.attname "
                        + "FROM pg_index i "
                        + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                        + "JOIN pg_class c ON i.indrelid = c.oid "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE i.indisprimary = true "
                        + "AND n.nspname = ? AND c.relname = ? "
                        + "ORDER BY array_position(i.indkey, a.attnum)");

        pg10Queries.put(
                QueryType.FIND_CHILD_PARTITIONS,
                "SELECT cn.nspname as child_schema, cc.relname as child_table "
                        + "FROM pg_inherits i "
                        + "JOIN pg_class pc ON i.inhparent = pc.oid "
                        + "JOIN pg_namespace pn ON pc.relnamespace = pn.oid "
                        + "JOIN pg_class cc ON i.inhrelid = cc.oid "
                        + "JOIN pg_namespace cn ON cc.relnamespace = cn.oid "
                        + "WHERE pn.nspname = ? AND pc.relname = ? "
                        + "AND cc.relispartition = true");

        pg10Queries.put(
                QueryType.CHECK_PUBLICATION_EXISTS,
                "SELECT COUNT(1) FROM pg_publication WHERE pubname = ?");

        pg10Queries.put(
                QueryType.FIND_PARTITIONED_TABLES_BY_FILTER,
                "SELECT schemaname, tablename "
                        + "FROM pg_tables t "
                        + "WHERE (%s) "
                        + "AND EXISTS ("
                        + "  SELECT 1 FROM pg_class c "
                        + "  JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "  WHERE n.nspname = t.schemaname "
                        + "  AND c.relname = t.tablename "
                        + "  AND c.relkind = 'p'"
                        + ")");

        pg10Queries.put(
                QueryType.GET_SCHEMA_TABLES,
                "SELECT n.nspname AS schema_name, c.relname AS table_name "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE c.relkind = 'r' "
                        + "AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast') "
                        + "ORDER BY n.nspname, c.relname");

        pg10Queries.put(
                QueryType.FIND_CHILD_PARTITIONS_ALT,
                "SELECT n.nspname AS schema_name, c.relname AS table_name "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "JOIN pg_inherits i ON c.oid = i.inhrelid "
                        + "JOIN pg_class parent ON i.inhparent = parent.oid "
                        + "JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid "
                        + "WHERE parent_ns.nspname = ? "
                        + "AND parent.relname = ? "
                        + "AND c.relkind = 'r'");

        versionQueries.put(PostgresVersionStrategy.V10, Collections.unmodifiableMap(pg10Queries));

        // PostgreSQL 11+ queries (same as PG10 for now, but can be optimized separately)
        Map<QueryType, String> pg11PlusQueries = new HashMap<>(pg10Queries);

        // PostgreSQL 11+ can use more advanced features if needed
        // For now, we use the same queries as they work well across versions
        versionQueries.put(
                PostgresVersionStrategy.V11_PLUS, Collections.unmodifiableMap(pg11PlusQueries));

        VERSION_QUERIES = Collections.unmodifiableMap(versionQueries);
    }

    /**
     * Get the appropriate SQL query for the given query type and PostgreSQL connection. This method
     * automatically detects the PostgreSQL version and returns the most suitable query.
     *
     * @param queryType the type of query to retrieve
     * @param connection the PostgreSQL connection (used for version detection)
     * @return the SQL query string
     * @throws IllegalArgumentException if the query type is not supported for the detected version
     */
    public static String getQuery(QueryType queryType, PostgresConnection connection) {
        PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
        return getQuery(queryType, strategy);
    }

    /**
     * Get the SQL query for the given query type and version strategy. This method allows explicit
     * version strategy specification.
     *
     * @param queryType the type of query to retrieve
     * @param strategy the PostgreSQL version strategy
     * @return the SQL query string
     * @throws IllegalArgumentException if the query type is not supported for the given strategy
     */
    public static String getQuery(QueryType queryType, PostgresVersionStrategy strategy) {
        Map<QueryType, String> queries = VERSION_QUERIES.get(strategy);
        if (queries == null) {
            throw new IllegalArgumentException(
                    "Unsupported PostgreSQL version strategy: " + strategy);
        }

        String query = queries.get(queryType);
        if (query == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query type %s is not supported for PostgreSQL version strategy %s",
                            queryType, strategy));
        }

        LOG.trace("Retrieved {} query for strategy {}: {}", queryType, strategy, query);
        return query;
    }

    /**
     * Check if a query type is supported for the given PostgreSQL connection.
     *
     * @param queryType the query type to check
     * @param connection the PostgreSQL connection
     * @return true if the query type is supported, false otherwise
     */
    public static boolean isQuerySupported(QueryType queryType, PostgresConnection connection) {
        try {
            getQuery(queryType, connection);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Check if a query type is supported for the given version strategy.
     *
     * @param queryType the query type to check
     * @param strategy the PostgreSQL version strategy
     * @return true if the query type is supported, false otherwise
     */
    public static boolean isQuerySupported(QueryType queryType, PostgresVersionStrategy strategy) {
        try {
            getQuery(queryType, strategy);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Get all supported query types for the given PostgreSQL connection.
     *
     * @param connection the PostgreSQL connection
     * @return a map of supported query types and their SQL strings
     */
    public static Map<QueryType, String> getAllQueries(PostgresConnection connection) {
        PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
        return getAllQueries(strategy);
    }

    /**
     * Get all supported query types for the given version strategy.
     *
     * @param strategy the PostgreSQL version strategy
     * @return a map of supported query types and their SQL strings
     */
    public static Map<QueryType, String> getAllQueries(PostgresVersionStrategy strategy) {
        Map<QueryType, String> queries = VERSION_QUERIES.get(strategy);
        if (queries == null) {
            throw new IllegalArgumentException(
                    "Unsupported PostgreSQL version strategy: " + strategy);
        }
        return new HashMap<>(queries); // Return a copy to prevent modification
    }

    /**
     * Get information about the query management system for debugging purposes.
     *
     * @return a string representation of the query management state
     */
    public static String getQueryManagementInfo() {
        StringBuilder info = new StringBuilder();
        info.append("PostgresSqlQueries Management Info:\n");

        for (PostgresVersionStrategy strategy : PostgresVersionStrategy.values()) {
            Map<QueryType, String> queries = VERSION_QUERIES.get(strategy);
            if (queries != null) {
                info.append(String.format("  %s: %d queries\n", strategy.name(), queries.size()));
                for (QueryType queryType : queries.keySet()) {
                    info.append(String.format("    - %s\n", queryType.name()));
                }
            }
        }

        return info.toString();
    }

    // Private constructor to prevent instantiation
    private PostgresSqlQueries() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
}
