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

package org.apache.flink.cdc.connectors.postgres.source.enumerator;

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** PostgreSQL-version regression tests for partitioned-table filtered publications. */
class PostgresPartitionedPublicationVersionITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresPartitionedPublicationVersionITCase.class);
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";
    private static final List<PostgresVersionFixture> POSTGRES_FIXTURES =
            Arrays.asList(
                    new PostgresVersionFixture("pg10", "postgres:10"),
                    new PostgresVersionFixture("pg11", "postgres:11"),
                    new PostgresVersionFixture("pg12", "postgres:12"),
                    new PostgresVersionFixture("pg13", "postgres:13"),
                    new PostgresVersionFixture("pg14", "postgres:14"),
                    new PostgresVersionFixture("pg15", "postgres:15"),
                    new PostgresVersionFixture("pg16", "postgres:16"));

    private final List<CleanupAction> cleanupActions = new ArrayList<>();

    @AfterAll
    static void stopContainers() {
        POSTGRES_FIXTURES.forEach(fixture -> fixture.container.stop());
    }

    @AfterEach
    void cleanup() {
        for (int i = cleanupActions.size() - 1; i >= 0; i--) {
            cleanupActions.get(i).cleanup();
        }
        cleanupActions.clear();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postgresVersions")
    void filteredPublicationCreatesChildOnlyMembersBeforeGlobalSlot(PostgresVersionFixture fixture)
            throws Exception {
        PartitionedDatabase database = createPartitionedDatabase(fixture);

        PostgresDialect dialect = new PostgresDialect(database.configFactory.create(0));
        PostgresSourceEnumerator.createSlotForGlobalStreamSplit(dialect);

        assertThat(dialect.routingState().allParents()).containsExactly(parentTableId(database));
        assertThat(dialect.routingState().allChildren())
                .containsExactlyInAnyOrderElementsOf(expectedChildTableIds(database));
        assertThat(
                        directPublicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrderElementsOf(expectedChildPublicationTables(database));
        assertThat(
                        publicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrderElementsOf(expectedChildPublicationTables(database));
        assertThat(
                        replicationSlotExists(
                                fixture.container,
                                database.databaseName,
                                database.slotName,
                                "pgoutput"))
                .isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postgresVersions")
    void existingSlotStillSeedsPartitionRoutingBeforeReturning(PostgresVersionFixture fixture)
            throws Exception {
        PartitionedDatabase database = createPartitionedDatabase(fixture);
        createLogicalReplicationSlot(fixture.container, database.databaseName, database.slotName);
        assertThat(
                        directPublicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .isEmpty();

        PostgresDialect restoredDialect = new PostgresDialect(database.configFactory.create(0));
        PostgresSourceEnumerator.createSlotForGlobalStreamSplit(restoredDialect);

        assertThat(restoredDialect.routingState().allParents())
                .containsExactly(parentTableId(database));
        assertThat(restoredDialect.routingState().allChildren())
                .containsExactlyInAnyOrderElementsOf(expectedChildTableIds(database));
        assertThat(
                        restoredDialect
                                .routingState()
                                .routeToLogicalTable(
                                        new TableId(
                                                null,
                                                database.schemaName,
                                                database.parentTable + "_uk")))
                .isEqualTo(parentTableId(database));
        assertThat(
                        directPublicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrderElementsOf(expectedChildPublicationTables(database));
        assertThat(
                        publicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrderElementsOf(expectedChildPublicationTables(database));
        assertThat(
                        replicationSlotExists(
                                fixture.container,
                                database.databaseName,
                                database.slotName,
                                "pgoutput"))
                .isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postgresVersions")
    void publicationRefreshAddsRuntimeChildPublicationMember(PostgresVersionFixture fixture)
            throws Exception {
        PartitionedDatabase database = createPartitionedDatabase(fixture, "filtered", true);

        PostgresDialect dialect = new PostgresDialect(database.configFactory.create(0));
        PostgresSourceEnumerator.createSlotForGlobalStreamSplit(dialect);
        assertThat(
                        directPublicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrderElementsOf(expectedChildPublicationTables(database));

        createRuntimePartition(fixture.container, database.databaseName, database, "china");

        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(database.configFactory.create(0), dialect);
        assertThat(context.refreshNewPartitionPublicationChildren())
                .containsExactly(
                        new TableId(null, database.schemaName, database.parentTable + "_china"));
        assertThat(dialect.routingState().allChildren())
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(
                                new TableId(
                                        null, database.schemaName, database.parentTable + "_uk"),
                                new TableId(
                                        null, database.schemaName, database.parentTable + "_us"),
                                new TableId(
                                        null,
                                        database.schemaName,
                                        database.parentTable + "_china")));
        assertThat(
                        directPublicationTables(
                                fixture.container, database.databaseName, database.publicationName))
                .containsExactlyInAnyOrder(
                        database.schemaName + "." + database.parentTable + "_uk",
                        database.schemaName + "." + database.parentTable + "_us",
                        database.schemaName + "." + database.parentTable + "_china");
    }

    static Stream<PostgresVersionFixture> postgresVersions() {
        return POSTGRES_FIXTURES.stream().peek(PostgresVersionFixture::start);
    }

    private PartitionedDatabase createPartitionedDatabase(PostgresVersionFixture fixture)
            throws Exception {
        return createPartitionedDatabase(fixture, "filtered", false);
    }

    private PartitionedDatabase createPartitionedDatabase(
            PostgresVersionFixture fixture,
            String publicationAutocreateMode,
            boolean partitionPublicationRefreshEnabled)
            throws Exception {
        String databaseName = uniqueIdentifier("db");
        String schemaName = "inventory_partitioned";
        String parentTable = "products";
        String publicationName = uniqueIdentifier("pub");
        String slotName = uniqueIdentifier("slot");
        createDatabase(fixture.container, databaseName);
        cleanupActions.add(() -> dropDatabase(fixture.container, databaseName));
        initializePartitionedProducts(fixture.container, databaseName, schemaName, parentTable);
        createEmptyPublication(fixture.container, databaseName, publicationName);
        cleanupActions.add(() -> dropReplicationSlot(fixture.container, databaseName, slotName));
        return new PartitionedDatabase(
                databaseName,
                schemaName,
                parentTable,
                publicationName,
                slotName,
                partitionedConfigFactory(
                        fixture.container,
                        databaseName,
                        schemaName,
                        parentTable,
                        publicationName,
                        slotName,
                        publicationAutocreateMode,
                        partitionPublicationRefreshEnabled));
    }

    private static PostgresSourceConfigFactory partitionedConfigFactory(
            PostgreSQLContainer<?> container,
            String databaseName,
            String schemaName,
            String parentTable,
            String publicationName,
            String slotName,
            String publicationAutocreateMode,
            boolean partitionPublicationRefreshEnabled) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(container.getHost());
        configFactory.port(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        configFactory.username(USER);
        configFactory.password(PASSWORD);
        configFactory.database(databaseName);
        configFactory.schemaList(new String[] {schemaName});
        configFactory.tableList(schemaName + "." + parentTable);
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");
        configFactory.setIncludePartitionedTables(true);
        configFactory.setPartitionPublicationRefreshEnabled(partitionPublicationRefreshEnabled);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(PostgresConnectorConfig.PUBLICATION_NAME.name(), publicationName);
        dbzProperties.setProperty(
                PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(),
                publicationAutocreateMode);
        configFactory.debeziumProperties(dbzProperties);
        return configFactory;
    }

    private static void createDatabase(PostgreSQLContainer<?> container, String databaseName)
            throws Exception {
        try (Connection connection =
                        PostgresTestBase.getJdbcConnection(container, PostgresTestBase.DEFAULT_DB);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
    }

    private static void dropDatabase(PostgreSQLContainer<?> container, String databaseName) {
        try (Connection connection =
                        PostgresTestBase.getJdbcConnection(container, PostgresTestBase.DEFAULT_DB);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '"
                            + databaseName
                            + "'");
            statement.execute("DROP DATABASE IF EXISTS " + databaseName);
        } catch (Exception e) {
            LOG.warn("Failed to drop PostgreSQL test database {}", databaseName, e);
        }
    }

    private static void initializePartitionedProducts(
            PostgreSQLContainer<?> container,
            String databaseName,
            String schemaName,
            String parentTable)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schemaName);
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ("
                                    + "id INT NOT NULL, country VARCHAR(20) NOT NULL) "
                                    + "PARTITION BY LIST(country)",
                            schemaName, parentTable));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s_uk PARTITION OF %s.%s FOR VALUES IN ('uk')",
                            schemaName, parentTable, schemaName, parentTable));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s_us PARTITION OF %s.%s FOR VALUES IN ('us')",
                            schemaName, parentTable, schemaName, parentTable));
        }
    }

    private static void createEmptyPublication(
            PostgreSQLContainer<?> container, String databaseName, String publicationName)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE PUBLICATION " + publicationName);
        }
    }

    private static void createLogicalReplicationSlot(
            PostgreSQLContainer<?> container, String databaseName, String slotName)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                            slotName));
        }
    }

    private static void createRuntimePartition(
            PostgreSQLContainer<?> container,
            String databaseName,
            PartitionedDatabase database,
            String country)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s_%s PARTITION OF %s.%s FOR VALUES IN ('%s')",
                            database.schemaName,
                            database.parentTable,
                            country,
                            database.schemaName,
                            database.parentTable,
                            country));
        }
    }

    private static void dropReplicationSlot(
            PostgreSQLContainer<?> container, String databaseName, String slotName) {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "SELECT pg_drop_replication_slot('%s') "
                                    + "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')",
                            slotName, slotName));
        } catch (Exception e) {
            LOG.warn("Failed to drop PostgreSQL test replication slot {}", slotName, e);
        }
    }

    private static List<String> publicationTables(
            PostgreSQLContainer<?> container, String databaseName, String publicationName)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT schemaname, tablename FROM pg_publication_tables "
                                                + "WHERE pubname = '%s'",
                                        publicationName))) {
            List<String> publicationTables = new ArrayList<>();
            while (rs.next()) {
                publicationTables.add(rs.getString("schemaname") + "." + rs.getString("tablename"));
            }
            return publicationTables;
        }
    }

    private static List<String> directPublicationTables(
            PostgreSQLContainer<?> container, String databaseName, String publicationName)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT ns.nspname, c.relname FROM pg_publication p "
                                                + "JOIN pg_publication_rel pr ON pr.prpubid = p.oid "
                                                + "JOIN pg_class c ON c.oid = pr.prrelid "
                                                + "JOIN pg_namespace ns ON ns.oid = c.relnamespace "
                                                + "WHERE p.pubname = '%s'",
                                        publicationName))) {
            List<String> publicationTables = new ArrayList<>();
            while (rs.next()) {
                publicationTables.add(rs.getString("nspname") + "." + rs.getString("relname"));
            }
            return publicationTables;
        }
    }

    private static boolean replicationSlotExists(
            PostgreSQLContainer<?> container, String databaseName, String slotName, String plugin)
            throws Exception {
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT 1 FROM pg_replication_slots "
                                                + "WHERE slot_name = '%s' AND plugin = '%s'",
                                        slotName, plugin))) {
            return rs.next();
        }
    }

    private static String uniqueIdentifier(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static TableId parentTableId(PartitionedDatabase database) {
        return new TableId(null, database.schemaName, database.parentTable);
    }

    private static List<TableId> expectedChildTableIds(PartitionedDatabase database) {
        return Arrays.asList(
                new TableId(null, database.schemaName, database.parentTable + "_uk"),
                new TableId(null, database.schemaName, database.parentTable + "_us"));
    }

    private static List<String> expectedChildPublicationTables(PartitionedDatabase database) {
        return Arrays.asList(
                database.schemaName + "." + database.parentTable + "_uk",
                database.schemaName + "." + database.parentTable + "_us");
    }

    private interface CleanupAction {
        void cleanup();
    }

    private static class PartitionedDatabase {

        private final String databaseName;
        private final String schemaName;
        private final String parentTable;
        private final String publicationName;
        private final String slotName;
        private final PostgresSourceConfigFactory configFactory;

        private PartitionedDatabase(
                String databaseName,
                String schemaName,
                String parentTable,
                String publicationName,
                String slotName,
                PostgresSourceConfigFactory configFactory) {
            this.databaseName = databaseName;
            this.schemaName = schemaName;
            this.parentTable = parentTable;
            this.publicationName = publicationName;
            this.slotName = slotName;
            this.configFactory = configFactory;
        }
    }

    private static class PostgresVersionFixture {

        private final String name;
        private final PostgreSQLContainer<?> container;
        private boolean started;

        private PostgresVersionFixture(String name, String imageName) {
            this.name = name;
            this.container =
                    new PostgreSQLContainer<>(
                                    DockerImageName.parse(imageName)
                                            .asCompatibleSubstituteFor("postgres"))
                            .withDatabaseName(PostgresTestBase.DEFAULT_DB)
                            .withUsername(USER)
                            .withPassword(PASSWORD)
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix(name))
                            .withCommand(
                                    "postgres",
                                    "-c",
                                    "fsync=off",
                                    "-c",
                                    "max_replication_slots=20",
                                    "-c",
                                    "wal_level=logical");
        }

        private void start() {
            if (!started) {
                Startables.deepStart(Stream.of(container)).join();
                started = true;
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
