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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory class for creating PostgreSQL schema readers based on database version and configuration.
 * This factory encapsulates the logic for selecting the appropriate schema reader implementation
 * and provides caching to improve performance.
 *
 * <p>The factory automatically detects the PostgreSQL version and selects the most suitable schema
 * reader implementation:
 *
 * <ul>
 *   <li>PostgreSQL 10: Uses {@link CustomPostgresSchemaV10} with partition table enhancements
 *   <li>PostgreSQL 11+: Uses {@link CustomPostgresSchema} with standard schema reading
 * </ul>
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Automatic version detection and strategy selection
 *   <li>Schema reader instance caching for performance
 *   <li>Thread-safe operations
 *   <li>Comprehensive error handling and logging
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * PostgresSchemaReaderFactory factory = new PostgresSchemaReaderFactory();
 * PostgresSchemaReader reader = factory.createReader(connection, config);
 *
 * // Or use the static convenience method
 * PostgresSchemaReader reader = PostgresSchemaReaderFactory.create(connection, config);
 * }</pre>
 */
public class PostgresSchemaReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaReaderFactory.class);

    // Cache for schema reader instances to avoid repeated creation
    private final ConcurrentMap<String, PostgresSchemaReader> readerCache =
            new ConcurrentHashMap<>();

    // Singleton instance for convenience
    private static final PostgresSchemaReaderFactory INSTANCE = new PostgresSchemaReaderFactory();

    /**
     * Create a new factory instance. Multiple instances can be created if needed, but consider
     * using the singleton instance via {@link #getInstance()} for better performance.
     */
    public PostgresSchemaReaderFactory() {
        LOG.debug("Created new PostgresSchemaReaderFactory instance");
    }

    /**
     * Get the singleton factory instance.
     *
     * @return the singleton factory instance
     */
    public static PostgresSchemaReaderFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Static convenience method to create a schema reader. This method uses the singleton factory
     * instance.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @return a new or cached schema reader instance
     */
    public static PostgresSchemaReader create(
            PostgresConnection connection, PostgresSourceConfig config) {
        return getInstance().createReader(connection, config);
    }

    /**
     * Static convenience method to create a schema reader with pre-computed table type information.
     * This method enables optimized schema reading by providing table type cache upfront.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @param tableTypes map of table identifiers to their types for caching
     * @return a new or cached schema reader instance with optimized table type cache
     */
    public static PostgresSchemaReader createWithTableTypes(
            PostgresConnection connection,
            PostgresSourceConfig config,
            java.util.Map<io.debezium.relational.TableId, PostgresTableType> tableTypes) {
        return getInstance().createReaderWithTableTypes(connection, config, tableTypes);
    }

    /**
     * Create a schema reader for the given connection and configuration. This method automatically
     * detects the PostgreSQL version and selects the appropriate schema reader implementation.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @return a schema reader instance (may be cached)
     * @throws IllegalArgumentException if connection or config is null
     * @throws RuntimeException if the schema reader cannot be created
     */
    public PostgresSchemaReader createReader(
            PostgresConnection connection, PostgresSourceConfig config) {

        if (connection == null) {
            throw new IllegalArgumentException("PostgreSQL connection cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("PostgreSQL source config cannot be null");
        }

        // Create cache key based on connection and config properties
        String cacheKey = createCacheKey(connection, config);

        return readerCache.computeIfAbsent(
                cacheKey,
                key -> {
                    LOG.debug("Creating new schema reader for cache key: {}", key);

                    try {
                        // Detect version strategy and create reader
                        PostgresVersionStrategy strategy =
                                PostgresVersionStrategy.forConnection(connection);
                        PostgresSchemaReader reader = strategy.createReader(connection, config);

                        LOG.info(
                                "Successfully created schema reader: {} using strategy: {}",
                                reader.getClass().getSimpleName(),
                                strategy.name());

                        return reader;

                    } catch (Exception e) {
                        LOG.error("Failed to create schema reader for cache key: {}", key, e);
                        throw new RuntimeException(
                                "Failed to create PostgreSQL schema reader: " + e.getMessage(), e);
                    }
                });
    }

    /**
     * Create a schema reader with explicit version strategy. This method bypasses automatic version
     * detection and uses the provided strategy.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @param strategy the version strategy to use
     * @return a schema reader instance
     * @throws IllegalArgumentException if any parameter is null
     */
    public PostgresSchemaReader createReader(
            PostgresConnection connection,
            PostgresSourceConfig config,
            PostgresVersionStrategy strategy) {

        if (connection == null) {
            throw new IllegalArgumentException("PostgreSQL connection cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("PostgreSQL source config cannot be null");
        }
        if (strategy == null) {
            throw new IllegalArgumentException("PostgreSQL version strategy cannot be null");
        }

        LOG.debug("Creating schema reader with explicit strategy: {}", strategy.name());

        try {
            PostgresSchemaReader reader = strategy.createReader(connection, config);

            LOG.info(
                    "Successfully created schema reader: {} using explicit strategy: {}",
                    reader.getClass().getSimpleName(),
                    strategy.name());

            return reader;

        } catch (Exception e) {
            LOG.error("Failed to create schema reader with strategy: {}", strategy.name(), e);
            throw new RuntimeException(
                    "Failed to create PostgreSQL schema reader: " + e.getMessage(), e);
        }
    }

    /**
     * Clear the schema reader cache. This will force creation of new instances on subsequent calls.
     * Useful for testing or when connection properties change.
     */
    public void clearCache() {
        int cacheSize = readerCache.size();
        readerCache.clear();
        LOG.info("Cleared schema reader cache, removed {} entries", cacheSize);
    }

    /**
     * Get the current cache size. Useful for monitoring and testing.
     *
     * @return the number of cached schema reader instances
     */
    public int getCacheSize() {
        return readerCache.size();
    }

    /**
     * Check if a schema reader is cached for the given connection and config.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @return true if a reader is cached, false otherwise
     */
    public boolean isCached(PostgresConnection connection, PostgresSourceConfig config) {
        String cacheKey = createCacheKey(connection, config);
        return readerCache.containsKey(cacheKey);
    }

    /**
     * Create a cache key based on connection and configuration properties. This ensures that
     * different connections or configurations get separate cache entries.
     */
    private String createCacheKey(PostgresConnection connection, PostgresSourceConfig config) {
        StringBuilder keyBuilder = new StringBuilder();

        try {
            // Include connection identifier
            keyBuilder.append("conn:").append(connection.connectionString());
        } catch (Exception e) {
            // Fallback to connection hash if connection string is not available
            keyBuilder.append("conn:").append(connection.hashCode());
        }

        // Include relevant config properties that affect schema reading
        keyBuilder
                .append("|db:")
                .append(
                        config.getDatabaseList().isEmpty()
                                ? "default"
                                : config.getDatabaseList().get(0));
        keyBuilder.append("|tables:").append(String.join(",", config.getTableList()));

        // Include partition mapping if present
        if (config.getPartitionMapping() != null && !config.getPartitionMapping().isEmpty()) {
            keyBuilder.append("|partitions:").append(config.getPartitionMapping().hashCode());
        }

        return keyBuilder.toString();
    }

    /**
     * Create a schema reader with pre-computed table type information for optimized performance.
     * This method creates a new reader and pre-populates its table type cache to avoid redundant
     * database queries during schema operations.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @param tableTypes map of table identifiers to their types for caching
     * @return a schema reader instance with optimized table type cache
     * @throws IllegalArgumentException if any parameter is null
     */
    public PostgresSchemaReader createReaderWithTableTypes(
            PostgresConnection connection,
            PostgresSourceConfig config,
            java.util.Map<io.debezium.relational.TableId, PostgresTableType> tableTypes) {

        if (connection == null) {
            throw new IllegalArgumentException("PostgreSQL connection cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("PostgreSQL source config cannot be null");
        }
        if (tableTypes == null) {
            throw new IllegalArgumentException("Table types map cannot be null");
        }

        LOG.debug("Creating schema reader with {} pre-computed table types", tableTypes.size());

        try {
            // Detect version strategy and create reader
            PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
            PostgresSchemaReader reader = strategy.createReader(connection, config);

            // Pre-populate table type cache for optimized performance
            reader.setTableTypeCache(tableTypes);

            LOG.info(
                    "Successfully created schema reader: {} with {} cached table types using strategy: {}",
                    reader.getClass().getSimpleName(),
                    tableTypes.size(),
                    strategy.name());

            return reader;

        } catch (Exception e) {
            LOG.error("Failed to create schema reader with table types", e);
            throw new RuntimeException(
                    "Failed to create PostgreSQL schema reader with table types: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Create a schema reader and automatically populate table type cache by discovering tables.
     * This method performs table discovery with type information and pre-populates the cache.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @param database the database name to discover tables from
     * @return a schema reader instance with automatically populated table type cache
     * @throws IllegalArgumentException if any parameter is null
     */
    public PostgresSchemaReader createReaderWithAutoTableTypes(
            PostgresConnection connection, PostgresSourceConfig config, String database) {

        if (connection == null) {
            throw new IllegalArgumentException("PostgreSQL connection cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("PostgreSQL source config cannot be null");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }

        LOG.debug(
                "Creating schema reader with automatic table type discovery for database: {}",
                database);

        try {
            // Discover tables with type information
            java.util.Set<TableInfo> tablesWithType =
                    TableDiscoveryUtils.listTablesWithType(
                            database, connection, config.getTableFilters());

            // Convert to map for caching
            java.util.Map<io.debezium.relational.TableId, PostgresTableType> tableTypes =
                    tablesWithType.stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            TableInfo::getTableId, TableInfo::getTableType));

            LOG.info(
                    "Auto-discovered {} tables with type information for schema reader",
                    tableTypes.size());

            // Create reader with pre-populated cache
            return createReaderWithTableTypes(connection, config, tableTypes);

        } catch (Exception e) {
            LOG.error("Failed to create schema reader with auto table type discovery", e);
            throw new RuntimeException(
                    "Failed to create PostgreSQL schema reader with auto table types: "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Get information about the factory state for debugging purposes.
     *
     * @return a string representation of the factory state
     */
    public String getFactoryInfo() {
        return String.format(
                "PostgresSchemaReaderFactory{cacheSize=%d, versionCacheSize=%d}",
                getCacheSize(), PostgresVersionStrategy.getCacheSize());
    }
}
