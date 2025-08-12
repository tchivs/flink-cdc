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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A connection pool factory to create pooled Postgres {@link HikariDataSource}. */
public class PostgresConnectionPoolFactory extends JdbcConnectionPoolFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresConnectionPoolFactory.class);

    public static final String JDBC_URL_PATTERN = "jdbc:postgresql://%s:%s/%s";

    // Performance optimization constants
    private static final long CONNECTION_TIMEOUT_MS = 30000; // 30 seconds
    private static final long IDLE_TIMEOUT_MS = 600000; // 10 minutes
    private static final long MAX_LIFETIME_MS = 1800000; // 30 minutes
    private static final long LEAK_DETECTION_THRESHOLD_MS = 60000; // 1 minute

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String database = sourceConfig.getDatabaseList().get(0);
        return String.format(JDBC_URL_PATTERN, hostName, port, database);
    }

    /**
     * The reuses of connection pools are based on databases in postgresql. Different databases in
     * same instance cannot reuse same connection pool to connect.
     */
    @Override
    public ConnectionPoolId getPoolId(
            JdbcConfiguration config, String dataSourcePoolFactoryIdentifier) {
        ConnectionPoolId poolId =
                new ConnectionPoolId(
                        config.getHostname(),
                        config.getPort(),
                        config.getHostname(),
                        config.getDatabase(),
                        dataSourcePoolFactoryIdentifier);

        LOG.debug("Created connection pool ID: {}", poolId);
        return poolId;
    }
    /**
     * Override to provide optimized HikariConfig for PostgreSQL. This method enhances connection
     * pool performance with PostgreSQL-specific optimizations.
     */
    //    @Override
    //    protected HikariConfig getHikariConfig(JdbcSourceConfig sourceConfig) {
    //        HikariConfig hikariConfig = super.getHikariConfig(sourceConfig);
    //        optimizeForPostgreSQL(hikariConfig, sourceConfig);
    //        return hikariConfig;
    //    }

    /** Apply PostgreSQL-specific optimizations to HikariConfig. */
    private void optimizeForPostgreSQL(HikariConfig config, JdbcSourceConfig sourceConfig) {
        // Connection timeout optimizations
        config.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        config.setIdleTimeout(IDLE_TIMEOUT_MS);
        config.setMaxLifetime(MAX_LIFETIME_MS);
        config.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD_MS);

        // PostgreSQL-specific connection properties for better performance
        config.addDataSourceProperty("tcpKeepAlive", "true");
        config.addDataSourceProperty("socketTimeout", "30");
        config.addDataSourceProperty("loginTimeout", "10");
        config.addDataSourceProperty("prepareThreshold", "5"); // Enable prepared statement caching
        config.addDataSourceProperty("preparedStatementCacheQueries", "256");
        config.addDataSourceProperty("preparedStatementCacheSizeMiB", "5");
        config.addDataSourceProperty(
                "defaultRowFetchSize", String.valueOf(sourceConfig.getFetchSize()));

        // Connection validation
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000); // 5 seconds

        // Pool sizing optimization based on connection pool size
        int poolSize = sourceConfig.getConnectionPoolSize();
        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(Math.max(1, poolSize / 4)); // Keep 25% as minimum idle

        LOG.info(
                "Optimized HikariConfig for PostgreSQL: poolSize={}, minIdle={}, "
                        + "connectionTimeout={}ms, idleTimeout={}ms, maxLifetime={}ms",
                poolSize,
                config.getMinimumIdle(),
                CONNECTION_TIMEOUT_MS,
                IDLE_TIMEOUT_MS,
                MAX_LIFETIME_MS);
    }
}
