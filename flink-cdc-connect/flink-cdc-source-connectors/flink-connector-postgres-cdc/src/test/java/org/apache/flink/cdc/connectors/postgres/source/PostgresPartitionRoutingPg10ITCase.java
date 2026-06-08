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

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresPartitionRoutingITCase.assertEqualsInAnyOrder;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresPartitionRoutingITCase.fetchRows;

/**
 * Regression tests that exercise the partition-routing publication path against a real
 * <b>PostgreSQL 10</b> server.
 *
 * <p>The shared {@link PostgresTestBase#POSTGRES_CONTAINER} runs PostgreSQL 14, which silently
 * accepts {@code CREATE PUBLICATION ... FOR TABLE <partitioned_parent>}. The bug fixed by {@code
 * PostgresReplicationConnection.determineCapturedTables} only manifests on PostgreSQL 10/11/12
 * where the server rejects partitioned parents in publications with:
 *
 * <pre>
 *   ERROR: "&lt;parent&gt;" is a partitioned table
 *   Detail: Adding partitioned tables to publications is not supported.
 * </pre>
 *
 * <p>This class therefore boots its own PG 10 container so that any future regression to the
 * "include parent in FOR TABLE" behavior is caught immediately, instead of only being noticed in
 * the field. PostgreSQL 10 still supports declarative partitioning ({@code PARTITION BY LIST}), so
 * the existing {@code inventory_partitioned.sql} fixture is reused unchanged.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class PostgresPartitionRoutingPg10ITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresPartitionRoutingPg10ITCase.class);

    private static final String SCHEMA_NAME = "inventory_partitioned_pg10";
    private static final String DEFAULT_DB = "postgres";
    private static final String TEST_USER = "postgres";
    private static final String TEST_PASSWORD = "postgres";
    private static final String PUBLICATION_NAME = "dbz_publication";
    private static final List<String> EXPECTED_SNAPSHOT =
            Arrays.asList(
                    "+I[101, scooter, Small 2-wheel scooter, 3.14, us]",
                    "+I[102, car battery, 12V car battery, 8.1, us]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, us]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.75, us]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875, us]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.0, uk]",
                    "+I[107, rocks, box of assorted rocks, 5.3, uk]",
                    "+I[108, jacket, water resistent black wind breaker, 0.1, uk]",
                    "+I[109, spare tire, 24 inch spare tire, 22.2, uk]");

    /**
     * PostgreSQL 10 image. The "alpine" variant keeps the container small while still supporting
     * logical decoding via the built-in {@code pgoutput} plugin.
     */
    private static final DockerImageName PG10_IMAGE =
            DockerImageName.parse("postgres:10").asCompatibleSubstituteFor("postgres");

    private static final Network NETWORK = Network.newNetwork();

    static final PostgreSQLContainer<?> PG10_CONTAINER =
            new PostgreSQLContainer<>(PG10_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername(TEST_USER)
                    .withPassword(TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres-pg10")
                    .withReuse(false)
                    .withCommand(
                            "postgres",
                            "-c",
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    private final UniqueDatabase partitionedDatabase =
            new UniqueDatabase(
                    PG10_CONTAINER, "postgres_pg10", SCHEMA_NAME, TEST_USER, TEST_PASSWORD);

    private String slotName;

    @BeforeAll
    static void startContainer() {
        LOG.info("Starting PG 10 container...");
        Startables.deepStart(Stream.of(PG10_CONTAINER)).join();
        LOG.info("PG 10 container started.");
    }

    @AfterAll
    static void stopContainer() {
        LOG.info("Stopping PG 10 container...");
        if (PG10_CONTAINER != null) {
            PG10_CONTAINER.stop();
        }
        LOG.info("PG 10 container stopped.");
    }

    @BeforeEach
    public void before() {
        partitionedDatabase.createAndInitialize();
        // unique slot per test so re-running does not collide on the persistent slot table
        this.slotName = "flink_pg10_" + new Random().nextInt(10000);
    }

    @AfterEach
    public void after() throws Exception {
        Thread.sleep(1000L);
        partitionedDatabase.removeSlot(slotName);
    }

    /**
     * On PG 10 the unfixed connector throws:
     *
     * <pre>
     *   ConnectException: Unable to create filtered publication dbz_publication for "..."
     *   Caused by: PSQLException: ERROR: "products" is a partitioned table
     *     Detail: Adding partitioned tables to publications is not supported.
     * </pre>
     *
     * <p>With the fix in place {@code determineCapturedTables} strips the partitioned parent from
     * the {@code FOR TABLE} list, the publication is created with only child partitions, and the
     * job runs to completion. This test would fail on PG 10 if the fix is reverted.
     */
    @ParameterizedTest
    @ValueSource(strings = {"initial", "latest-offset"})
    void testFilteredPublicationDoesNotIncludePartitionedParentOnPg10(String scanStartupMode)
            throws Exception {
        TableResult tableResult =
                startPartitionedProductsJob(
                        scanStartupMode,
                        "," + " 'debezium.publication.autocreate.mode' = 'filtered'");
        CloseableIterator<Row> iterator = tableResult.collect();
        try {
            if ("initial".equals(scanStartupMode)) {
                assertEqualsInAnyOrder(
                        EXPECTED_SNAPSHOT, fetchRows(iterator, EXPECTED_SNAPSHOT.size()));
            }

            assertFilteredPublicationContainsChildrenOnly(PUBLICATION_NAME);
            assertStreamingInsertRouted(iterator, 200, "glove", "leather glove", 0.3, "us");
        } finally {
            cancelJob(tableResult);
        }
    }

    @Test
    void testAllTablesPublicationRoutesPartitionChangesOnPg10() throws Exception {
        TableResult tableResult =
                startPartitionedProductsJob(
                        "latest-offset",
                        "," + " 'debezium.publication.autocreate.mode' = 'all_tables'");
        CloseableIterator<Row> iterator = tableResult.collect();
        try {
            waitUntilPublicationExists(PUBLICATION_NAME);
            assertStreamingInsertRouted(iterator, 201, "lamp", "desk lamp", 2.5, "us");
        } finally {
            cancelJob(tableResult);
        }
    }

    @Test
    void testDisabledPublicationUsesPrecreatedChildPublicationOnPg10() throws Exception {
        createChildOnlyPublication(PUBLICATION_NAME);

        TableResult tableResult =
                startPartitionedProductsJob(
                        "latest-offset",
                        "," + " 'debezium.publication.autocreate.mode' = 'disabled'");
        CloseableIterator<Row> iterator = tableResult.collect();
        try {
            assertFilteredPublicationContainsChildrenOnly(PUBLICATION_NAME);
            assertStreamingInsertRouted(iterator, 202, "mug", "coffee mug", 0.4, "uk");
        } finally {
            cancelJob(tableResult);
        }
    }

    @Test
    void testCommittedOffsetWithPrecreatedPublicationOnPg10() throws Exception {
        createChildOnlyPublication(PUBLICATION_NAME);
        createLogicalReplicationSlot(slotName);

        TableResult tableResult =
                startPartitionedProductsJob(
                        "committed-offset",
                        "," + " 'debezium.publication.autocreate.mode' = 'disabled'");
        CloseableIterator<Row> iterator = tableResult.collect();
        try {
            assertFilteredPublicationContainsChildrenOnly(PUBLICATION_NAME);
            assertStreamingInsertRouted(iterator, 204, "after", "inserted after start", 1.1, "us");
        } finally {
            cancelJob(tableResult);
        }
    }

    @Test
    void testSnapshotStartupRoutesPartitionedParentOnPg10() throws Exception {
        TableResult tableResult = startPartitionedProductsJob("snapshot", "");
        try {
            assertEqualsInAnyOrder(
                    EXPECTED_SNAPSHOT, fetchRows(tableResult.collect(), EXPECTED_SNAPSHOT.size()));
        } finally {
            cancelJob(tableResult);
        }
    }

    // ==================== helpers ====================

    private TableResult startPartitionedProductsJob(String scanStartupMode, String extraOptions) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        // Fail fast: do not retry forever if PG 10 rejects publication setup.
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 0, 0);

        tEnv.executeSql(buildSourceDDL(scanStartupMode, extraOptions));
        return tEnv.executeSql("SELECT * FROM partitioned_products");
    }

    private String buildSourceDDL(String scanStartupMode, String extraOptions) {
        return format(
                "CREATE TABLE partitioned_products ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight FLOAT,"
                        + " country STRING"
                        // Intentionally no PRIMARY KEY: PG 10 forbids PKs on partitioned parents,
                        // so the source must work without one (matching real-world PG 10 schemas).
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = 'products',"
                        + " 'scan.startup.mode' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '100',"
                        + " 'decoding.plugin.name' = 'pgoutput',"
                        + " 'slot.name' = '%s',"
                        + " 'scan.include-partitioned-tables.enabled' = 'true',"
                        + " 'scan.lsn-commit.checkpoints-num-delay' = '1'"
                        + "%s"
                        + ")",
                partitionedDatabase.getHost(),
                partitionedDatabase.getDatabasePort(),
                partitionedDatabase.getUsername(),
                partitionedDatabase.getPassword(),
                partitionedDatabase.getDatabaseName(),
                SCHEMA_NAME,
                scanStartupMode,
                slotName,
                extraOptions);
    }

    private void assertFilteredPublicationContainsChildrenOnly(String publicationName)
            throws Exception {
        // Wait until both pre-existing children are added to the publication. On the unfixed
        // code the underlying CREATE/ALTER PUBLICATION statement fails on PG 10, the source job
        // dies before any child is published, and this wait will time out.
        waitUntilPublished(publicationName, "products_us");
        waitUntilPublished(publicationName, "products_uk");

        // Publication membership invariant: the partitioned parent must NOT be a member.
        Assertions.assertThat(isPublished(publicationName, "products"))
                .as(
                        "partitioned parent 'products' must not be a publication member on PG 10. "
                                + "PostgreSQL 10 rejects 'CREATE/ALTER PUBLICATION ... FOR TABLE "
                                + "<parent>' with: \"<parent>\" is a partitioned table.")
                .isFalse();
    }

    private void waitUntilPublicationExists(String publicationName) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (publicationExists(publicationName)) {
                return;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError(format("Timed out waiting for publication %s", publicationName));
    }

    private void waitUntilPublished(String publicationName, String tableName) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        long continuouslyPublishedSince = -1L;
        while (System.currentTimeMillis() < deadline) {
            if (isPublished(publicationName, tableName)) {
                if (continuouslyPublishedSince < 0) {
                    continuouslyPublishedSince = System.currentTimeMillis();
                }
                if (System.currentTimeMillis() - continuouslyPublishedSince >= 2000L) {
                    return;
                }
            } else {
                continuouslyPublishedSince = -1L;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError(
                format(
                        "Timed out waiting for %s.%s to be added to publication %s on PG 10",
                        SCHEMA_NAME, tableName, publicationName));
    }

    private boolean publicationExists(String publicationName) throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement();
                ResultSet rs =
                        stat.executeQuery(
                                format(
                                        "SELECT 1 FROM pg_publication WHERE pubname = '%s'",
                                        publicationName))) {
            return rs.next();
        }
    }

    private boolean isPublished(String publicationName, String tableName) throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement();
                ResultSet rs =
                        stat.executeQuery(
                                format(
                                        "SELECT 1 FROM pg_publication_tables "
                                                + "WHERE pubname = '%s' "
                                                + "AND schemaname = '%s' "
                                                + "AND tablename = '%s'",
                                        publicationName, SCHEMA_NAME, tableName))) {
            return rs.next();
        }
    }

    private void waitUntilReplicationSlotActive(String slotName) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (isReplicationSlotActive(slotName)) {
                return;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError(format("Timed out waiting for replication slot %s", slotName));
    }

    private boolean isReplicationSlotActive(String slotName) throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement();
                ResultSet rs =
                        stat.executeQuery(
                                format(
                                        "SELECT active FROM pg_replication_slots "
                                                + "WHERE slot_name = '%s'",
                                        slotName))) {
            return rs.next() && rs.getBoolean("active");
        }
    }

    private void createChildOnlyPublication(String publicationName) throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    format(
                            "CREATE PUBLICATION %s FOR TABLE %s.products_us, %s.products_uk",
                            publicationName, SCHEMA_NAME, SCHEMA_NAME));
        }
    }

    private void createLogicalReplicationSlot(String slotName) throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    format("select pg_create_logical_replication_slot('%s','pgoutput')", slotName));
        }
    }

    private void assertStreamingInsertRouted(
            CloseableIterator<Row> iterator,
            int id,
            String name,
            String description,
            double weight,
            String country)
            throws Exception {
        waitUntilReplicationSlotActive(slotName);
        insertProduct(id, name, description, weight, country);

        List<String> results = fetchRows(iterator, 1);
        assertEqualsInAnyOrder(
                Arrays.asList(
                        format("+I[%s, %s, %s, %s, %s]", id, name, description, weight, country)),
                results);
    }

    private void insertProduct(
            int id, String name, String description, double weight, String country)
            throws Exception {
        try (Connection conn =
                        PostgresTestBase.getJdbcConnection(
                                PG10_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);
            stat.execute(
                    format(
                            "INSERT INTO products VALUES (%s, '%s', '%s', %s, '%s')",
                            id,
                            quoteSqlLiteralValue(name),
                            quoteSqlLiteralValue(description),
                            weight,
                            country));
        }
    }

    private String quoteSqlLiteralValue(String value) {
        return value.replace("'", "''");
    }

    private void cancelJob(TableResult tableResult) {
        tableResult
                .getJobClient()
                .ifPresent(
                        client -> {
                            try {
                                client.cancel().get();
                            } catch (Exception e) {
                                // ignore
                            }
                        });
    }
}
