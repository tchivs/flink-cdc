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

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Strategy enum for managing different PostgreSQL versions and their corresponding schema reader
 * implementations. This provides a centralized way to handle version-specific logic and eliminates
 * the need for scattered version checks throughout the codebase.
 *
 * <p>Each strategy encapsulates:
 *
 * <ul>
 *   <li>Minimum PostgreSQL version requirement
 *   <li>Corresponding schema reader implementation class
 *   <li>Version detection logic
 *   <li>Factory method for creating schema readers
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
 * PostgresSchemaReader reader = strategy.createReader(connection, config);
 * }</pre>
 */
public enum PostgresVersionStrategy {

    /**
     * Strategy for PostgreSQL 10.x Uses CustomPostgresSchemaV10 which handles PostgreSQL 10
     * specific limitations such as partition table primary key constraints.
     */
    V10(10, CustomPostgresSchemaV10.class, "PostgreSQL 10.x with partition table enhancements"),

    /**
     * Strategy for PostgreSQL 11 and above Uses CustomPostgresSchema which leverages standard
     * schema reading capabilities available in PostgreSQL 11+.
     */
    V11_PLUS(11, CustomPostgresSchema.class, "PostgreSQL 11+ with standard schema reading");

    private static final Logger LOG = LoggerFactory.getLogger(PostgresVersionStrategy.class);

    // Cache for version detection results to avoid repeated database queries
    private static final ConcurrentMap<String, PostgresVersionStrategy> VERSION_CACHE =
            new ConcurrentHashMap<>();

    private final int minVersion;
    private final Class<? extends PostgresSchemaReader> readerClass;
    private final String description;

    PostgresVersionStrategy(
            int minVersion, Class<? extends PostgresSchemaReader> readerClass, String description) {
        this.minVersion = minVersion;
        this.readerClass = readerClass;
        this.description = description;
    }

    /**
     * Determine the appropriate strategy for the given PostgreSQL connection. Results are cached to
     * avoid repeated version detection queries.
     *
     * @param connection the PostgreSQL connection
     * @return the appropriate version strategy
     */
    public static PostgresVersionStrategy forConnection(PostgresConnection connection) {
        // Create a cache key based on connection properties
        String cacheKey = createCacheKey(connection);

        return VERSION_CACHE.computeIfAbsent(
                cacheKey,
                key -> {
                    boolean isV11OrLater = PostgresVersionUtils.isServer11OrLater(connection);
                    PostgresVersionStrategy strategy = isV11OrLater ? V11_PLUS : V10;

                    LOG.info(
                            "Detected PostgreSQL version strategy: {} for connection {}",
                            strategy.description,
                            key);

                    return strategy;
                });
    }

    /**
     * Create a schema reader instance for this version strategy.
     *
     * @param connection the PostgreSQL connection
     * @param config the source configuration
     * @return a new schema reader instance
     * @throws RuntimeException if the reader cannot be created
     */
    public PostgresSchemaReader createReader(
            PostgresConnection connection, PostgresSourceConfig config) {
        try {
            Constructor<? extends PostgresSchemaReader> constructor =
                    readerClass.getConstructor(
                            PostgresConnection.class, PostgresSourceConfig.class);

            PostgresSchemaReader reader = constructor.newInstance(connection, config);

            LOG.debug(
                    "Created schema reader: {} for strategy: {}",
                    readerClass.getSimpleName(),
                    this.name());

            return reader;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to create schema reader for strategy %s: %s",
                            this.name(), e.getMessage()),
                    e);
        }
    }

    /** Get the minimum PostgreSQL version for this strategy. */
    public int getMinVersion() {
        return minVersion;
    }

    /** Get the schema reader class for this strategy. */
    public Class<? extends PostgresSchemaReader> getReaderClass() {
        return readerClass;
    }

    /** Get the description of this strategy. */
    public String getDescription() {
        return description;
    }

    /**
     * Check if this strategy supports the given PostgreSQL version.
     *
     * @param version the PostgreSQL major version
     * @return true if this strategy supports the version
     */
    public boolean supportsVersion(int version) {
        if (this == V10) {
            return version == 10;
        } else {
            return version >= minVersion;
        }
    }

    /**
     * Clear the version detection cache. Useful for testing or when connection properties change.
     */
    public static void clearCache() {
        VERSION_CACHE.clear();
        LOG.debug("Cleared PostgreSQL version strategy cache");
    }

    /** Get the current cache size. Useful for monitoring and testing. */
    public static int getCacheSize() {
        return VERSION_CACHE.size();
    }

    /**
     * Create a cache key based on connection properties. This helps ensure we cache results per
     * unique connection.
     */
    private static String createCacheKey(PostgresConnection connection) {
        try {
            // Use connection string or other unique identifier
            return connection.connectionString();
        } catch (Exception e) {
            // Fallback to object hash if connection string is not available
            LOG.warn(
                    "Could not get connection string for caching, using object hash: {}",
                    e.getMessage());
            return String.valueOf(connection.hashCode());
        }
    }

    @Override
    public String toString() {
        return String.format(
                "PostgresVersionStrategy{name=%s, minVersion=%d, readerClass=%s, description='%s'}",
                name(), minVersion, readerClass.getSimpleName(), description);
    }
}
