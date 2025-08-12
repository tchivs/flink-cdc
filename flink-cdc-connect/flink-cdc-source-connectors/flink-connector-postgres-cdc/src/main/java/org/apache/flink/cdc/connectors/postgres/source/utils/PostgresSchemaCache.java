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
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global schema cache to minimize redundant readSchema calls across PostgreSQL CDC operations. This
 * cache significantly reduces the schema reading overhead, especially in scenarios with many tables
 * or frequent split processing.
 *
 * <p>Key benefits:
 *
 * <ul>
 *   <li>Reduces schema reading time from minutes to seconds for large table sets
 *   <li>Eliminates redundant database metadata queries
 *   <li>Improves data reading throughput by minimizing idle time
 *   <li>Supports concurrent access from multiple fetch tasks
 * </ul>
 */
public class PostgresSchemaCache {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaCache.class);

    // Global cache instance - shared across all PostgreSQL source instances
    private static final PostgresSchemaCache INSTANCE = new PostgresSchemaCache();

    // Cache for parsed Tables objects by database/config combination
    private final ConcurrentMap<String, CachedTables> schemaCache = new ConcurrentHashMap<>();

    // Cache statistics
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong refreshCount = new AtomicLong(0);

    // Cache expiration time (5 minutes by default)
    private static final long CACHE_TTL_MS = 5 * 60 * 1000;

    private PostgresSchemaCache() {
        // Singleton
    }

    public static PostgresSchemaCache getInstance() {
        return INSTANCE;
    }

    /**
     * Gets or creates cached Tables for the given PostgreSQL connection and configuration. This
     * method implements the core optimization to avoid redundant readSchema calls.
     *
     * @param connection the PostgreSQL connection
     * @param databaseName the database name
     * @param schemaName the schema name (can be null for all schemas)
     * @param tableFilter the table filter to apply
     * @param refreshSchema whether to force refresh the schema
     * @return cached or newly loaded Tables object
     * @throws SQLException if schema reading fails
     */
    public Tables getOrLoadTables(
            PostgresConnection connection,
            String databaseName,
            String schemaName,
            io.debezium.relational.Tables.TableFilter tableFilter,
            boolean refreshSchema)
            throws SQLException {

        String cacheKey = buildCacheKey(databaseName, schemaName, connection.connectionString());
        CachedTables cached = schemaCache.get(cacheKey);

        // Check if we have a valid cached entry
        if (!refreshSchema && cached != null && !cached.isExpired()) {
            cacheHits.incrementAndGet();
            LOG.debug("Schema cache hit for key: {}", cacheKey);
            return cached.tables;
        }

        // Cache miss or expired - need to read schema
        cacheMisses.incrementAndGet();
        LOG.info("Schema cache miss for key: {}, refreshing from database", cacheKey);

        Tables tables = new Tables();
        long startTime = System.currentTimeMillis();

        try {
            // Perform the expensive readSchema operation
            connection.readSchema(tables, databaseName, schemaName, tableFilter, null, false);

            long duration = System.currentTimeMillis() - startTime;
            LOG.info(
                    "Schema reading completed in {}ms for database: {}, found {} tables",
                    duration,
                    databaseName,
                    tables.size());

            // Cache the results
            schemaCache.put(cacheKey, new CachedTables(tables, System.currentTimeMillis()));
            refreshCount.incrementAndGet();

            return tables;

        } catch (SQLException e) {
            LOG.error(
                    "Failed to read schema for database: {}, schema: {}",
                    databaseName,
                    schemaName,
                    e);
            throw e;
        }
    }

    /**
     * Checks if a specific table schema is cached and valid.
     *
     * @param tableId the table identifier
     * @param connection the PostgreSQL connection
     * @param databaseName the database name
     * @return true if the table schema is cached and valid
     */
    public boolean isTableSchemaCached(
            TableId tableId, PostgresConnection connection, String databaseName) {
        String cacheKey =
                buildCacheKey(databaseName, tableId.schema(), connection.connectionString());
        CachedTables cached = schemaCache.get(cacheKey);

        if (cached != null && !cached.isExpired()) {
            return cached.tables.forTable(tableId) != null;
        }

        return false;
    }

    /**
     * Invalidates cache entries for a specific database or connection. This is useful when schema
     * changes are detected.
     *
     * @param connection the PostgreSQL connection
     * @param databaseName the database name to invalidate
     */
    public void invalidateCache(PostgresConnection connection, String databaseName) {
        String keyPattern = buildCacheKey(databaseName, null, connection.connectionString());
        schemaCache.entrySet().removeIf(entry -> entry.getKey().startsWith(keyPattern));
        LOG.info("Invalidated schema cache for database: {}", databaseName);
    }

    /** Clears all cached schema entries. Useful for testing or when global schema changes occur. */
    public void clearCache() {
        int size = schemaCache.size();
        schemaCache.clear();
        LOG.info("Cleared schema cache, removed {} entries", size);
    }

    /**
     * Gets cache statistics for monitoring and debugging.
     *
     * @return formatted cache statistics string
     */
    public String getCacheStats() {
        long hits = cacheHits.get();
        long misses = cacheMisses.get();
        long total = hits + misses;
        double hitRatio = total > 0 ? (double) hits / total : 0.0;

        return String.format(
                "PostgresSchemaCache stats: hits=%d, misses=%d, hit_ratio=%.2f%%, refreshes=%d, cached_entries=%d",
                hits, misses, hitRatio * 100, refreshCount.get(), schemaCache.size());
    }

    /** Builds a unique cache key for the given parameters. */
    private String buildCacheKey(String databaseName, String schemaName, String connectionString) {
        return String.format(
                "%s|%s|%s",
                databaseName != null ? databaseName : "",
                schemaName != null ? schemaName : "",
                connectionString != null ? connectionString.hashCode() : "");
    }

    /** Internal class to hold cached Tables with timestamp for TTL management. */
    private static class CachedTables {
        final Tables tables;
        final long timestamp;

        CachedTables(Tables tables, long timestamp) {
            this.tables = tables;
            this.timestamp = timestamp;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }
    }
}
