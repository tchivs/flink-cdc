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

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.test.util.AbstractTestBase;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Test base class for PG10-specific PostgreSQL CDC tests. This provides a dedicated PG10 container
 * for testing features specific to PostgreSQL 10 (e.g., partition support, publication behavior).
 */
public abstract class PostgresPg10TestBase extends AbstractTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(PostgresPg10TestBase.class);
    public static final String DEFAULT_DB = "postgres";
    public static final String TEST_USER = "postgres";
    public static final String TEST_PASSWORD = "postgres";

    protected static final DockerImageName PG10_IMAGE =
            DockerImageName.parse("postgres:10").asCompatibleSubstituteFor("postgres");
    public static final Network NETWORK = Network.newNetwork();
    public static final String INTER_CONTAINER_POSTGRES_ALIAS = "postgres10";

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG10_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername(TEST_USER)
                    .withPassword(TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_POSTGRES_ALIAS)
                    .withReuse(false)
                    .withCommand(
                            "postgres",
                            "-c",
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    @BeforeAll
    static void startContainers() throws Exception {
        LOG.info("Starting PG10 containers...");
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        LOG.info("PG10 containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping PG10 containers...");
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.stop();
        }
        LOG.info("PG10 containers are stopped.");
    }

    protected Connection getJdbcConnection(PostgreSQLContainer container) throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    public static Connection getJdbcConnection(PostgreSQLContainer container, String databaseName)
            throws SQLException {
        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        container.getHost(),
                        container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        databaseName);
        return DriverManager.getConnection(
                jdbcUrl, container.getUsername(), container.getPassword());
    }

    public static String getSlotName() {
        final Random random = new Random();
        int id = random.nextInt(10000);
        return "flink_" + id;
    }

    protected PostgresConnection createConnection(java.util.Map<String, String> properties) {
        Configuration config = Configuration.from(properties);
        return new PostgresConnection(JdbcConfiguration.adapt(config), "test-connection");
    }

    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database, String schemaName, String tableName, int splitSize) {
        return getMockPostgresSourceConfigFactory(
                database, schemaName, tableName, null, splitSize, false);
    }

    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database,
            String schemaName,
            String tableName,
            @Nullable String slotName,
            int splitSize,
            boolean skipSnapshotBackfill) {

        PostgresSourceConfigFactory postgresSourceConfigFactory = new PostgresSourceConfigFactory();
        postgresSourceConfigFactory.hostname(database.getHost());
        postgresSourceConfigFactory.port(database.getDatabasePort());
        postgresSourceConfigFactory.username(database.getUsername());
        postgresSourceConfigFactory.password(database.getPassword());
        postgresSourceConfigFactory.database(database.getDatabaseName());
        postgresSourceConfigFactory.schemaList(new String[] {schemaName});
        postgresSourceConfigFactory.tableList(schemaName + "." + tableName);
        postgresSourceConfigFactory.splitSize(splitSize);
        postgresSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        postgresSourceConfigFactory.setLsnCommitCheckpointsDelay(1);
        postgresSourceConfigFactory.decodingPluginName("pgoutput");
        if (slotName != null) {
            postgresSourceConfigFactory.slotName(slotName);
        }

        return postgresSourceConfigFactory;
    }
}
