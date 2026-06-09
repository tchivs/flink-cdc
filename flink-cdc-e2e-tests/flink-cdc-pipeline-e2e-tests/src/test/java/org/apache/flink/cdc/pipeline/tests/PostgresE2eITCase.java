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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PaimonE2eTestUtils;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.postgres.PostgresTestBase.getJdbcConnection;
import static org.apache.flink.cdc.connectors.postgres.PostgresTestBase.getSlotName;

/** End-to-end tests for postgres cdc pipeline job. */
public class PostgresE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresE2eITCase.class);
    private static final Duration PAIMON_TESTCASE_TIMEOUT = Duration.ofMinutes(3);
    private static final String PARTITIONED_INVENTORY_SCHEMA = "inventory_partitioned";

    private static final List<String> PARTITIONED_PRODUCTS_SNAPSHOT_ROWS =
            Arrays.asList(
                    "101, scooter, Small 2-wheel scooter, 3.14, us",
                    "102, car battery, 12V car battery, 8.1, us",
                    "103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, us",
                    "104, hammer, 12oz carpenter's hammer, 0.75, us",
                    "105, hammer, 14oz carpenter's hammer, 0.875, us",
                    "106, hammer, 16oz carpenter's hammer, 1.0, uk",
                    "107, rocks, box of assorted rocks, 5.3, uk",
                    "108, jacket, water resistent black wind breaker, 0.1, uk",
                    "109, spare tire, 24 inch spare tire, 22.2, uk");

    private static final List<String> PARTITIONED_PRODUCTS_STREAM_ROWS =
            Arrays.asList(
                    "110, jacket, water resistent white wind breaker, 0.2, us",
                    "111, scooter, Big 2-wheel scooter, 5.18, uk");

    // ------------------------------------------------------------------------------------------
    // Postgres Variables (we always use Postgres as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String POSTGRES_TEST_USER = "postgres";
    protected static final String POSTGRES_TEST_PASSWORD = "postgres";
    protected static final String INTER_CONTAINER_POSTGRES_ALIAS = "postgres";

    // use official postgresql image to support pgoutput plugin
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("postgres:14").asCompatibleSubstituteFor("postgres");

    @Container
    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetworkAliases(INTER_CONTAINER_POSTGRES_ALIAS)
                    .withNetwork(NETWORK)
                    .withUsername(POSTGRES_TEST_USER)
                    .withPassword(POSTGRES_TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical")
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(30L));

    private final UniqueDatabase postgresInventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "postgres_inventory",
                    POSTGRES_TEST_USER,
                    POSTGRES_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter = (s) -> String.format(s, "inventory");
    private String slotName;

    @BeforeEach
    public void before() throws Exception {
        super.before();
        slotName = getSlotName();
        postgresInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        postgresInventoryDatabase.removeSlot(slotName);
        postgresInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.inventory.products,%s.inventory.customers\n"
                                + "  slot.name: %s\n"
                                + "  scan.startup.mode: initial\n"
                                + "  server-time-zone: UTC\n"
                                + "  connect.timeout: 120s\n"
                                + "  schema-change.enabled: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_POSTGRES_ALIAS,
                        5432,
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD,
                        postgresInventoryDatabase.getDatabaseName(),
                        postgresInventoryDatabase.getDatabaseName(),
                        slotName,
                        parallelism);
        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL 'nextval('inventory.customers_id_seq'::regclass)',`first_name` VARCHAR(255) NOT NULL,`last_name` VARCHAR(255) NOT NULL,`email` VARCHAR(255) NOT NULL}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, Anne, Kretchmar, annek@noanswer.org], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, Edward, Walker, ed@walker.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, George, Bailey, gbailey@foobar.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, Sally, Thomas, sally.thomas@acme.com], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL 'nextval('inventory.products_id_seq'::regclass)',`name` VARCHAR(255) NOT NULL,`description` VARCHAR(512),`weight` FLOAT}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, postgresInventoryDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");

            // Perform DML changes after the wal log is generated
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.products, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.products, before=[107, rocks, box of assorted rocks, 5.3], after=[107, rocks, box of assorted rocks, 5.1], op=UPDATE, meta=()}");
        } catch (Exception e) {
            LOG.error("Update table for CDC failed.", e);
            throw new RuntimeException(e);
        }

        LOG.info("Begin schema change stage.");

        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, postgresInventoryDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            // Test ADD COLUMN
            stat.execute("ALTER TABLE inventory.products ADD COLUMN category VARCHAR(255);");
            stat.execute(
                    "INSERT INTO inventory.products VALUES (default, 'widget', 'A small widget', 1.5, 'tools');");

            // Test DROP COLUMN
            stat.execute("ALTER TABLE inventory.products DROP COLUMN weight;");
            stat.execute(
                    "INSERT INTO inventory.products VALUES (default, 'gadget', 'A useful gadget', 'electronics');");

            // Test RENAME COLUMN
            stat.execute(
                    "ALTER TABLE inventory.products RENAME COLUMN category TO product_category;");
            stat.execute(
                    "INSERT INTO inventory.products VALUES (default, 'gizmo', 'A fancy gizmo', 'gadgets');");
        } catch (Exception e) {
            LOG.error("Schema change test failed.", e);
            throw new RuntimeException(e);
        }

        // Validate schema change events and corresponding data
        waitUntilSpecificEvent(
                "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`category` VARCHAR(255), position=LAST, existedColumnName=null}]}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.products, before=[], after=[110, widget, A small widget, 1.5, tools], op=INSERT, meta=()}");

        waitUntilSpecificEvent(
                "DropColumnEvent{tableId=inventory.products, droppedColumnNames=[weight]}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.products, before=[], after=[111, gadget, A useful gadget, electronics], op=INSERT, meta=()}");

        waitUntilSpecificEvent(
                "RenameColumnEvent{tableId=inventory.products, nameMapping={category=product_category}}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.products, before=[], after=[112, gizmo, A fancy gizmo, gadgets], op=INSERT, meta=()}");
    }

    @Test
    void testPartitionedTableSnapshotAndStreamingToPaimon() throws Exception {
        runPartitionedTableToPaimon(
                PartitionedPaimonScenario.defaultInitial("default_initial_paimon"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("partitionPublicationPaimonScenarios")
    void testPartitionedTablePublicationAutocreateModeToPaimon(PartitionedPaimonScenario scenario)
            throws Exception {
        runPartitionedTableToPaimon(scenario);
    }

    static Stream<Arguments> partitionPublicationPaimonScenarios() {
        return Stream.of(
                Arguments.of(
                        PartitionedPaimonScenario.filtered("filtered_initial_paimon", "initial")),
                Arguments.of(
                        PartitionedPaimonScenario.filtered(
                                "filtered_latest_offset_paimon", "latest-offset")),
                Arguments.of(
                        PartitionedPaimonScenario.disabledRefresh(
                                "disabled_refresh_initial_paimon", "initial")),
                Arguments.of(
                        PartitionedPaimonScenario.disabledRefresh(
                                "disabled_refresh_latest_offset_paimon", "latest-offset")));
    }

    private void runPartitionedTableToPaimon(PartitionedPaimonScenario scenario) throws Exception {
        UniqueDatabase postgresPartitionedInventoryDatabase =
                new UniqueDatabase(
                        POSTGRES_CONTAINER,
                        "postgres_partitioned_paimon",
                        PARTITIONED_INVENTORY_SCHEMA,
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD);
        String partitionedSlotName = getSlotName();
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String publicationName =
                "dbz_pub_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);

        JobID jobID = null;
        try {
            postgresPartitionedInventoryDatabase.createAndInitialize();
            if (scenario.createEmptyPublication) {
                createEmptyPublication(
                        postgresPartitionedInventoryDatabase.getDatabaseName(), publicationName);
            }
            PaimonE2eTestUtils.copySqlClientDependencies(
                    jobManager, sharedVolume.toString(), flinkVersion);

            Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
            Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
            Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
            jobID =
                    submitPipelineJob(
                            buildPartitionedPaimonPipelineJob(
                                    postgresPartitionedInventoryDatabase.getDatabaseName(),
                                    partitionedSlotName,
                                    publicationName,
                                    warehouse,
                                    scenario),
                            postgresCdcJar,
                            paimonCdcConnector,
                            hadoopJar);
            waitUntilJobRunning(Duration.ofSeconds(30));
            LOG.info("Postgres partitioned table to Paimon pipeline job is running");

            if ("initial".equals(scenario.startupMode)) {
                validatePaimonSinkResult(
                        warehouse,
                        PARTITIONED_INVENTORY_SCHEMA,
                        "products",
                        PARTITIONED_PRODUCTS_SNAPSHOT_ROWS);
                assertPaimonTableDoesNotExist(
                        warehouse, PARTITIONED_INVENTORY_SCHEMA, "products_us");
                assertPaimonTableDoesNotExist(
                        warehouse, PARTITIONED_INVENTORY_SCHEMA, "products_uk");
            } else {
                Thread.sleep(10000L);
            }

            insertPartitionedPaimonStreamRows(
                    postgresPartitionedInventoryDatabase.getDatabaseName());

            validatePaimonSinkResult(
                    warehouse, PARTITIONED_INVENTORY_SCHEMA, "products", scenario.expectedRows());
            assertPaimonTableDoesNotExist(warehouse, PARTITIONED_INVENTORY_SCHEMA, "products_us");
            assertPaimonTableDoesNotExist(warehouse, PARTITIONED_INVENTORY_SCHEMA, "products_uk");
        } finally {
            if (jobID != null) {
                try {
                    cancelJob(jobID);
                } catch (Exception e) {
                    LOG.warn("Failed to cancel pipeline job {}.", jobID, e);
                }
            }
            postgresPartitionedInventoryDatabase.removeSlot(partitionedSlotName);
            dropSchemaCascade(
                    postgresPartitionedInventoryDatabase.getDatabaseName(),
                    PARTITIONED_INVENTORY_SCHEMA);
        }
    }

    private String buildPartitionedPaimonPipelineJob(
            String databaseName,
            String slotName,
            String publicationName,
            String warehouse,
            PartitionedPaimonScenario scenario) {
        return String.format(
                "source:\n"
                        + "  type: postgres\n"
                        + "  hostname: %s\n"
                        + "  port: %d\n"
                        + "  username: %s\n"
                        + "  password: %s\n"
                        + "  tables: %s.%s.products\n"
                        + "  slot.name: %s\n"
                        + "  scan.startup.mode: %s\n"
                        + "  scan.incremental.snapshot.chunk.size: 2\n"
                        + "  scan.incremental.snapshot.backfill.skip: true\n"
                        + "  scan.incremental.close-idle-reader.enabled: true\n"
                        + "  scan.include-partitioned-tables.enabled: true\n"
                        + "  server-time-zone: UTC\n"
                        + "  connect.timeout: 120s\n"
                        + "  schema-change.enabled: true\n"
                        + scenario.publicationConfig(publicationName)
                        + "\n"
                        + "sink:\n"
                        + "  type: paimon\n"
                        + "  catalog.properties.warehouse: %s\n"
                        + "  catalog.properties.metastore: filesystem\n"
                        + "  catalog.properties.cache-enabled: false\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  schema.change.behavior: evolve\n"
                        + "  parallelism: %d",
                INTER_CONTAINER_POSTGRES_ALIAS,
                5432,
                POSTGRES_TEST_USER,
                POSTGRES_TEST_PASSWORD,
                databaseName,
                PARTITIONED_INVENTORY_SCHEMA,
                slotName,
                scenario.startupMode,
                warehouse,
                parallelism);
    }

    private void dropSchemaCascade(String database, String schema) {
        try (Connection conn = getJdbcConnection(POSTGRES_CONTAINER, database);
                Statement stat = conn.createStatement()) {
            stat.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        } catch (Exception e) {
            LOG.warn("Failed to drop Postgres schema {}.{}.", database, schema, e);
        }
    }

    private void createEmptyPublication(String database, String publicationName) throws Exception {
        try (Connection conn = getJdbcConnection(POSTGRES_CONTAINER, database);
                Statement stat = conn.createStatement()) {
            stat.execute(String.format("CREATE PUBLICATION %s", publicationName));
        }
    }

    private void insertPartitionedPaimonStreamRows(String database) throws Exception {
        try (Connection conn = getJdbcConnection(POSTGRES_CONTAINER, database);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO inventory_partitioned.products VALUES "
                            + "(default,'jacket','water resistent white wind breaker',0.2, 'us');");
            stat.execute(
                    "INSERT INTO inventory_partitioned.products VALUES "
                            + "(default,'scooter','Big 2-wheel scooter',5.18, 'uk');");
        }
    }

    private void validatePaimonSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + PAIMON_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results =
                        PaimonE2eTestUtils.fetchTableRows(
                                jobManager,
                                sharedVolume.toString(),
                                flinkVersion,
                                warehouse,
                                database,
                                table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info("Successfully verified {} Paimon records.", expected.size());
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        logPaimonWarehouseLayout(warehouse);
        LOG.warn(
                "JobManager logs before Paimon validation failure:\n{}",
                jobManagerConsumer.toUtf8String());
        LOG.warn(
                "TaskManager logs before Paimon validation failure:\n{}",
                taskManagerConsumer.toUtf8String());
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    private void logPaimonWarehouseLayout(String warehouse) {
        try {
            org.testcontainers.containers.Container.ExecResult result =
                    jobManager.execInContainer("find", warehouse, "-maxdepth", "5", "-type", "f");
            LOG.warn(
                    "Paimon warehouse file layout for {}. Exit code: {}, stdout:\n{}\nstderr:\n{}",
                    warehouse,
                    result.getExitCode(),
                    result.getStdout(),
                    result.getStderr());
        } catch (Exception e) {
            LOG.warn("Failed to list Paimon warehouse {}.", warehouse, e);
        }
    }

    private void assertPaimonTableDoesNotExist(String warehouse, String database, String table)
            throws Exception {
        List<String> tables =
                PaimonE2eTestUtils.fetchTables(
                        jobManager, sharedVolume.toString(), flinkVersion, warehouse, database);
        Assertions.assertThat(tables)
                .as("Paimon tables in database " + database)
                .doesNotContain(table);
        LOG.info("Paimon table {}.{} does not exist as expected.", database, table);
    }

    private static final class PartitionedPaimonScenario {
        private final String name;
        private final String startupMode;
        private final String publicationAutocreateMode;
        private final boolean partitionPublicationRefreshEnabled;
        private final boolean createEmptyPublication;

        private PartitionedPaimonScenario(
                String name,
                String startupMode,
                String publicationAutocreateMode,
                boolean partitionPublicationRefreshEnabled,
                boolean createEmptyPublication) {
            this.name = name;
            this.startupMode = startupMode;
            this.publicationAutocreateMode = publicationAutocreateMode;
            this.partitionPublicationRefreshEnabled = partitionPublicationRefreshEnabled;
            this.createEmptyPublication = createEmptyPublication;
        }

        private static PartitionedPaimonScenario defaultInitial(String name) {
            return new PartitionedPaimonScenario(name, "initial", null, false, false);
        }

        private static PartitionedPaimonScenario filtered(String name, String startupMode) {
            return new PartitionedPaimonScenario(name, startupMode, "filtered", false, false);
        }

        private static PartitionedPaimonScenario disabledRefresh(String name, String startupMode) {
            return new PartitionedPaimonScenario(name, startupMode, "disabled", true, true);
        }

        private String publicationConfig(String publicationName) {
            if (publicationAutocreateMode == null) {
                return "";
            }
            return String.format(
                    "  debezium.publication.name: %s\n"
                            + "  debezium.publication.autocreate.mode: %s\n"
                            + "  scan.partition.publication.refresh.enabled: %s\n",
                    publicationName, publicationAutocreateMode, partitionPublicationRefreshEnabled);
        }

        private List<String> expectedRows() {
            if ("initial".equals(startupMode)) {
                List<String> rows = new ArrayList<>(PARTITIONED_PRODUCTS_SNAPSHOT_ROWS);
                rows.addAll(PARTITIONED_PRODUCTS_STREAM_ROWS);
                return rows;
            }
            return PARTITIONED_PRODUCTS_STREAM_ROWS;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
