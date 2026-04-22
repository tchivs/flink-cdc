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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.cdc.connectors.postgres.PostgresPg10TestBase;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.reader.PostgresSourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for PostgreSQL to start from a savepoint. */
class PostgreSQLSavepointITCase extends PostgresTestBase {

    @TempDir private Path tempDir;

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        PostgresSourceReader.clearSnapshotStreamSplitHook();
    }

    @Test
    void testSavepoint() throws Exception {
        testRestartFromSavepoint();
    }

    @Test
    void testRestartFromSavepointWithLatestOffset() throws Exception {
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");

        final String savepointDirectory = tempDir.toString();
        String finishedSavePointPath = null;
        final String slotName = getSlotName();
        final String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'decoding.plugin.name' = 'pgoutput',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        slotName);
        final String sinkDDL =
                "CREATE TABLE sink ("
                        + " id INT,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3)"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        JobClient jobClient = result.getJobClient().get();

        Thread.sleep(10000L);
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'latest_first','after startup',0.51);");
        }

        waitForRawResults(
                "latest-offset first phase consumed",
                rows -> rows.stream().anyMatch(row -> row.contains("latest_first")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();

        env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        jobClient = result.getJobClient().get();

        Thread.sleep(5000L);
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'latest_second','after restore',0.52);");
        }

        waitForRawResults(
                "latest-offset restore phase consumed",
                rows -> rows.stream().anyMatch(row -> row.contains("latest_second")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual.stream().filter(row -> row.contains("latest_first")).count())
                .isEqualTo(1);
        Assertions.assertThat(actual.stream().filter(row -> row.contains("latest_second")).count())
                .isEqualTo(1);
        Assertions.assertThat(actual)
                .noneMatch(row -> row.contains("Small 2-wheel scooter"))
                .noneMatch(row -> row.contains("car battery"));

        jobClient.cancel().get();
    }

    @Test
    void testRestartFromSavepointWithCommittedOffset() throws Exception {
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'history_before_slot','before slot',0.11);");
        }

        final String slotName = getSlotName();
        final String publicationName = "dbz_publication_savepoint_" + new Random().nextInt(1000);
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory.products", publicationName));
            statement.execute(
                    String.format(
                            "select pg_create_logical_replication_slot('%s','pgoutput');",
                            slotName));
        }

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'committed_first','after slot',0.21);");
        }

        final String savepointDirectory = tempDir.toString();
        String finishedSavePointPath = null;
        final String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'decoding.plugin.name' = 'pgoutput',"
                                + " 'slot.name' = '%s',"
                                + " 'debezium.publication.name' = '%s',"
                                + " 'scan.lsn-commit.checkpoints-num-delay' = '0',"
                                + " 'scan.startup.mode' = 'committed-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        slotName,
                        publicationName);
        final String sinkDDL =
                "CREATE TABLE sink ("
                        + " id INT,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3)"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        JobClient jobClient = result.getJobClient().get();

        waitForRawResults(
                "committed-offset first phase consumed",
                rows -> rows.stream().anyMatch(row -> row.contains("committed_first")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();

        env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        jobClient = result.getJobClient().get();

        Thread.sleep(5000L);
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'committed_second','after restore',0.22);");
        }

        waitForRawResults(
                "committed-offset restore phase consumed",
                rows -> rows.stream().anyMatch(row -> row.contains("committed_second")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(
                        actual.stream().filter(row -> row.contains("committed_first")).count())
                .isEqualTo(1);
        Assertions.assertThat(
                        actual.stream().filter(row -> row.contains("committed_second")).count())
                .isEqualTo(1);
        Assertions.assertThat(actual).noneMatch(row -> row.contains("history_before_slot"));

        jobClient.cancel().get();
    }

    @Test
    void testRestartFromSavepointPreservesAcceptedRuntimeChildRoutingPg10() throws Exception {
        ensurePg10ContainerStarted();

        final String databaseName = createUniquePg10DatabaseName("pg10_partitioned_savepoint");
        initializePg10PartitionedTable(databaseName);

        final String publicationName =
                "dbz_publication_pg10_savepoint_" + new Random().nextInt(1000);
        final String slotName = PostgresPg10TestBase.getSlotName();
        createPg10PublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        final String savepointDirectory = tempDir.toString();
        String finishedSavePointPath = null;
        final String preAcceptanceMarker =
                "pre_acceptance_savepoint_ca_" + new Random().nextInt(1_000_000);
        final String postAcceptanceMarker =
                "post_acceptance_savepoint_ca_" + new Random().nextInt(1_000_000);
        final String restoredMarker = "restored_savepoint_ca_" + new Random().nextInt(1_000_000);
        final String sourceDDL =
                createPg10PartitionedSourceDDL(databaseName, publicationName, slotName, false);
        final String sinkDDL = createPg10PartitionedSinkDDL();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");
        JobClient jobClient = result.getJobClient().get();

        long initialActivePid =
                waitForPg10ReplicationSlotActivePid(
                        databaseName, slotName, Duration.ofMinutes(2), Duration.ofSeconds(1));

        createRuntimePartition(databaseName, "products_ca", "ca");
        insertPartitionRow(
                databaseName,
                "products_ca",
                preAcceptanceMarker,
                "created_before_acceptance_before_savepoint",
                2.40,
                "ca");

        waitForPg10PublicationTable(
                databaseName,
                publicationName,
                "products_ca",
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        long acceptedActivePid =
                waitForPg10ReplicationSlotActivePidChange(
                        databaseName,
                        slotName,
                        initialActivePid,
                        Duration.ofMinutes(2),
                        Duration.ofSeconds(1));
        Assertions.assertThat(acceptedActivePid).isNotEqualTo(initialActivePid);

        List<String> rowsAfterAcceptance = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(countRowsContaining(rowsAfterAcceptance, preAcceptanceMarker))
                .isEqualTo(0L);

        insertPartitionRow(
                databaseName,
                "products_ca",
                postAcceptanceMarker,
                "created_after_acceptance_before_savepoint",
                2.60,
                "ca");

        waitForRawResults(
                "PG10 accepted runtime child post-acceptance streaming captured before savepoint",
                rows -> countRowsContaining(rows, postAcceptanceMarker) == 1,
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        waitForPg10RoutingMaterializedInSnapshotState(
                databaseName,
                Duration.ofMinutes(2),
                Duration.ofMillis(100),
                rows -> countRowsContaining(rows, postAcceptanceMarker) == 1);

        List<String> rowsBeforeSavepoint = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(countRowsContaining(rowsBeforeSavepoint, preAcceptanceMarker))
                .isEqualTo(0L);
        Assertions.assertThat(countRowsContaining(rowsBeforeSavepoint, postAcceptanceMarker))
                .isEqualTo(1L);
        assertParentMetadataRoutingOnly(
                rowsBeforeSavepoint, "products_uk", "products_us", "products_ca");

        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();

        env = getStreamExecutionEnvironment(finishedSavePointPath, 1);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");
        jobClient = result.getJobClient().get();

        long restoredActivePid =
                waitForPg10ReplicationSlotFirstActivePidAfterRestart(
                        databaseName,
                        slotName,
                        acceptedActivePid,
                        Duration.ofMinutes(2),
                        Duration.ofMillis(100));

        insertPartitionRow(
                databaseName,
                "products_ca",
                restoredMarker,
                "created_after_restore_without_rediscovery",
                2.80,
                "ca");

        List<String> actual =
                waitForRawResultsWhilePg10ReplicationSlotActivePidStable(
                        "restored PG10 job preserves accepted runtime child routing without rediscovery",
                        rows -> countRowsContaining(rows, restoredMarker) == 1,
                        databaseName,
                        slotName,
                        restoredActivePid,
                        Duration.ofMinutes(1),
                        Duration.ofMillis(200));

        assertPg10ReplicationSlotActivePidStable(
                databaseName,
                slotName,
                restoredActivePid,
                Duration.ofSeconds(5),
                Duration.ofMillis(500));

        Assertions.assertThat(countRowsContaining(actual, preAcceptanceMarker)).isEqualTo(0L);
        Assertions.assertThat(countRowsContaining(actual, postAcceptanceMarker)).isEqualTo(1L);
        Assertions.assertThat(countRowsContaining(actual, restoredMarker)).isEqualTo(1L);
        assertParentMetadataRoutingOnly(actual, "products_uk", "products_us", "products_ca");

        jobClient.cancel().get();
    }

    private void testRestartFromSavepoint() throws Exception {
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");

        final String savepointDirectory = tempDir.toString();
        String finishedSavePointPath = null;

        StreamExecutionEnvironment env = getStreamExecutionEnvironment(finishedSavePointPath, 4);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'decoding.plugin.name' = 'pgoutput', "
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        JobClient jobClient = result.getJobClient().get();
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        Thread.sleep(10000L);
        waitForSinkResult(
                "sink",
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                        "+I[102, car battery, 12V car battery, 8.100]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                        "+I[106, hammer, 16oz carpenter's hammer, 1.000]",
                        "+I[107, rocks, box of assorted rocks, 5.300]",
                        "+I[108, jacket, water resistent black wind breaker, 0.100]",
                        "+I[109, spare tire, 24 inch spare tire, 22.200]",
                        "+I[110, jacket, new water resistent white wind breaker, 0.500]"));

        finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        jobClient.cancel().get();

        env = getStreamExecutionEnvironment(finishedSavePointPath, 4);
        tEnv = StreamTableEnvironment.create(env);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 112
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=112;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=113;");
            statement.execute("DELETE FROM inventory.products WHERE id=113;");
        }

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        jobClient = result.getJobClient().get();

        waitForSinkSize("sink", 15);

        String[] expected =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                    "+I[102, car battery, 12V car battery, 8.100]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.000]",
                    "+I[107, rocks, box of assorted rocks, 5.300]",
                    "+I[108, jacket, water resistent black wind breaker, 0.100]",
                    "+I[109, spare tire, 24 inch spare tire, 22.200]",
                    "+I[110, jacket, new water resistent white wind breaker, 0.500]",
                    "+I[112, jacket, new water resistent white wind breaker, 0.500]"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        jobClient.cancel().get();
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavePointPath != null) {
            // restore from savepoint
            // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz =
                    classLoader.loadClass(
                            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
            Field field = clazz.getDeclaredField("configuration");
            field.setAccessible(true);
            Configuration configuration = (Configuration) field.get(env);
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        return env;
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        Exception lastRetryException = null;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                String savepointPath =
                        jobClient
                                .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                                .get();
                if (savepointPath == null || savepointPath.isBlank()) {
                    throw new AssertionError(
                            String.format(
                                    "Trigger savepoint completed without a valid path for directory %s",
                                    savepointDirectory));
                }
                return savepointPath;
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    lastRetryException = e;
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        throw new AssertionError(
                String.format(
                        "Failed to obtain savepoint path from directory %s after %s retry attempts",
                        savepointDirectory, retryTimes),
                lastRetryException);
    }

    private void waitForRawResults(
            String conditionName,
            Predicate<List<String>> condition,
            Duration timeout,
            Duration interval)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            List<String> rows;
            try {
                rows = TestValuesTableFactory.getRawResultsAsStrings("sink");
            } catch (IllegalArgumentException e) {
                rows = java.util.Collections.emptyList();
            }
            if (condition.test(rows)) {
                return;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        "Timeout waiting for condition: " + conditionName + ", rows: " + rows);
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private void ensurePg10ContainerStarted() {
        if (!PostgresPg10TestBase.POSTGRES_CONTAINER.isRunning()) {
            PostgresPg10TestBase.POSTGRES_CONTAINER.start();
        }
    }

    private String createUniquePg10DatabaseName(String logicalName) throws Exception {
        String databaseName = logicalName + "_" + Math.abs(new Random().nextInt(1_000_000));
        try (Connection connection = getJdbcConnection(PostgresPg10TestBase.POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
        return databaseName;
    }

    private void initializePg10PartitionedTable(String databaseName) throws Exception {
        try (Connection connection =
                        PostgresPg10TestBase.getJdbcConnection(
                                PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS inventory_partitioned CASCADE");
            statement.execute("CREATE SCHEMA inventory_partitioned");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products ("
                            + " id SERIAL NOT NULL,"
                            + " name VARCHAR(255) NOT NULL DEFAULT 'flink',"
                            + " description VARCHAR(512),"
                            + " weight FLOAT,"
                            + " country VARCHAR(20) NOT NULL"
                            + ") PARTITION BY LIST(country)");
            statement.execute(
                    "ALTER SEQUENCE inventory_partitioned.products_id_seq RESTART WITH 101");
            statement.execute("ALTER TABLE inventory_partitioned.products REPLICA IDENTITY FULL");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_uk PARTITION OF inventory_partitioned.products FOR VALUES IN ('uk')");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_us PARTITION OF inventory_partitioned.products FOR VALUES IN ('us')");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES "
                            + "(default,'scooter','Small 2-wheel scooter',3.14, 'us'),"
                            + "(default,'car battery','12V car battery',8.1, 'us'),"
                            + "(default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8, 'us'),"
                            + "(default,'hammer','12oz carpenter''s hammer',0.75, 'us'),"
                            + "(default,'hammer','14oz carpenter''s hammer',0.875, 'us'),"
                            + "(default,'hammer','16oz carpenter''s hammer',1.0, 'uk'),"
                            + "(default,'rocks','box of assorted rocks',5.3, 'uk'),"
                            + "(default,'jacket','water resistent black wind breaker',0.1, 'uk'),"
                            + "(default,'spare tire','24 inch spare tire',22.2, 'uk')");
        }
    }

    private void createPg10PublicationAndSlot(
            String databaseName, String publicationName, String slotName, String publicationTables)
            throws Exception {
        try (Connection connection =
                        PostgresPg10TestBase.getJdbcConnection(
                                PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE %s",
                            publicationName, publicationTables));
            statement.execute(
                    String.format(
                            "select pg_create_logical_replication_slot('%s','pgoutput');",
                            slotName));
        }
    }

    private String createPg10PartitionedSourceDDL(
            String databaseName,
            String publicationName,
            String slotName,
            boolean childPartitionBackfillEnabled) {
        return String.format(
                "CREATE TABLE debezium_source ("
                        + " schema_name STRING METADATA VIRTUAL,"
                        + " table_name STRING METADATA VIRTUAL,"
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.include-partitioned-tables.enabled' = 'true',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'scan.incremental.snapshot.chunk.key-column' = 'id',"
                        + " 'decoding.plugin.name' = 'pgoutput',"
                        + " 'debezium.publication.name' = '%s',"
                        + " 'slot.name' = '%s',"
                        + " 'scan.startup.mode' = 'latest-offset',"
                        + " 'scan.pg10.publication.poll.interval' = '1 s',"
                        + " 'scan.pg10.startup.fast-poll.interval' = '1 s',"
                        + " 'scan.pg10.startup.fast-poll.duration' = '10 s',"
                        + " 'scan.pg10.child-partition.backfill.enabled' = '%s'"
                        + ")",
                PostgresPg10TestBase.POSTGRES_CONTAINER.getHost(),
                PostgresPg10TestBase.POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                PostgresPg10TestBase.POSTGRES_CONTAINER.getUsername(),
                PostgresPg10TestBase.POSTGRES_CONTAINER.getPassword(),
                databaseName,
                "inventory_partitioned",
                "products",
                publicationName,
                slotName,
                Boolean.toString(childPartitionBackfillEnabled));
    }

    private String createPg10PartitionedSinkDDL() {
        return "CREATE TABLE sink ("
                + " schema_name STRING,"
                + " table_name STRING,"
                + " id INT,"
                + " name STRING,"
                + " country STRING"
                + ") WITH ("
                + " 'connector' = 'values',"
                + " 'sink-insert-only' = 'false'"
                + ")";
    }

    private void createRuntimePartition(String databaseName, String childTableName, String country)
            throws Exception {
        try (Connection connection =
                PostgresPg10TestBase.getJdbcConnection(
                        PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName)) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        String.format(
                                "CREATE TABLE inventory_partitioned.%s PARTITION OF inventory_partitioned.products FOR VALUES IN ('%s')",
                                childTableName, country));
            }
            connection.commit();
        }
    }

    private void insertPartitionRow(
            String databaseName,
            String childTableName,
            String name,
            String description,
            double weight,
            String country)
            throws Exception {
        try (Connection connection =
                        PostgresPg10TestBase.getJdbcConnection(
                                PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO inventory_partitioned.%s (name, description, weight, country) VALUES ('%s', '%s', %s, '%s');",
                            childTableName,
                            escapeSqlLiteral(name),
                            escapeSqlLiteral(description),
                            Double.toString(weight),
                            escapeSqlLiteral(country)));
        }
    }

    private String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private long waitForPg10ReplicationSlotActivePid(
            String databaseName, String slotName, Duration timeout, Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Long activePid = getPg10ReplicationSlotActivePid(databaseName, slotName);
            if (activePid != null) {
                return activePid;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for PG10 replication slot %s to become active",
                                slotName));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private long waitForPg10ReplicationSlotActivePidChange(
            String databaseName,
            String slotName,
            long previousActivePid,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Long activePid = getPg10ReplicationSlotActivePid(databaseName, slotName);
            if (activePid != null && activePid != previousActivePid) {
                return activePid;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for PG10 replication slot %s active_pid to change from %s; latest pid: %s",
                                slotName, previousActivePid, activePid));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private long waitForPg10ReplicationSlotFirstActivePidAfterRestart(
            String databaseName,
            String slotName,
            long previousActivePid,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        boolean previousSessionReleased = false;

        while (true) {
            Long activePid = getPg10ReplicationSlotActivePid(databaseName, slotName);
            if (!previousSessionReleased) {
                if (activePid == null) {
                    previousSessionReleased = true;
                } else if (activePid != previousActivePid) {
                    return activePid;
                }
            } else if (activePid != null) {
                return activePid;
            }

            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for PG10 replication slot %s to restart after previous active_pid %s; latest pid: %s",
                                slotName, previousActivePid, activePid));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private List<String> waitForRawResultsWhilePg10ReplicationSlotActivePidStable(
            String conditionName,
            Predicate<List<String>> condition,
            String databaseName,
            String slotName,
            long expectedActivePid,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Long activePid = getPg10ReplicationSlotActivePid(databaseName, slotName);
            if (activePid == null) {
                throw new AssertionError(
                        String.format(
                                "PG10 replication slot %s became inactive while waiting for condition: %s",
                                slotName, conditionName));
            }
            if (activePid != expectedActivePid) {
                throw new AssertionError(
                        String.format(
                                "Restored PG10 job required an additional rediscovery/restart before condition '%s' was met: expected slot %s active_pid to stay at %s, but observed %s",
                                conditionName, slotName, expectedActivePid, activePid));
            }

            List<String> rows;
            try {
                rows = TestValuesTableFactory.getRawResultsAsStrings("sink");
            } catch (IllegalArgumentException e) {
                rows = java.util.Collections.emptyList();
            }
            if (condition.test(rows)) {
                return rows;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        "Timeout waiting for condition: " + conditionName + ", rows: " + rows);
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private void assertPg10ReplicationSlotActivePidStable(
            String databaseName,
            String slotName,
            long expectedActivePid,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= timeout.toMillis()) {
            Long activePid = getPg10ReplicationSlotActivePid(databaseName, slotName);
            if (activePid == null) {
                throw new AssertionError(
                        String.format(
                                "PG10 replication slot %s became inactive while verifying restore stability",
                                slotName));
            }
            if (activePid != expectedActivePid) {
                throw new AssertionError(
                        String.format(
                                "Restored PG10 job regressed to rediscovery semantics: expected slot %s active_pid to stay at %s, but observed %s",
                                slotName, expectedActivePid, activePid));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private Long getPg10ReplicationSlotActivePid(String databaseName, String slotName)
            throws Exception {
        try (Connection connection =
                        PostgresPg10TestBase.getJdbcConnection(
                                PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement();
                java.sql.ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '%s' AND database = '%s'",
                                        slotName, databaseName))) {
            if (!resultSet.next()) {
                throw new AssertionError(
                        String.format(
                                "PG10 replication slot %s not found for database %s",
                                slotName, databaseName));
            }
            Object activePid = resultSet.getObject("active_pid");
            return activePid == null ? null : ((Number) activePid).longValue();
        }
    }

    private void waitForPg10PublicationTable(
            String databaseName,
            String publicationName,
            String tableName,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            if (pg10PublicationContainsTable(databaseName, publicationName, tableName)) {
                return;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for PG10 publication %s to contain table %s",
                                publicationName, tableName));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private boolean pg10PublicationContainsTable(
            String databaseName, String publicationName, String tableName) throws Exception {
        try (Connection connection =
                        PostgresPg10TestBase.getJdbcConnection(
                                PostgresPg10TestBase.POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement();
                java.sql.ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT 1 FROM pg_publication_tables WHERE pubname = '%s' AND schemaname = 'inventory_partitioned' AND tablename = '%s'",
                                        publicationName, tableName))) {
            return resultSet.next();
        }
    }

    private long countRowsContaining(List<String> rows, String token) {
        return rows.stream().filter(row -> row.contains(token)).count();
    }

    private void waitForPg10RoutingMaterializedInSnapshotState(
            String databaseName,
            Duration timeout,
            Duration interval,
            Predicate<List<String>> rawResultsReady)
            throws Exception {
        AtomicReference<String> materializedChildTable = new AtomicReference<>();
        PostgresSourceReader.setSnapshotStreamSplitHook(
                split -> {
                    if (!split.isPg10RoutingStateInitialized()) {
                        return;
                    }
                    split.getPg10ChildToParentMapping().keySet().stream()
                            .map(tableId -> tableId.table())
                            .filter("products_ca"::equals)
                            .findFirst()
                            .ifPresent(materializedChildTable::set);
                });

        try {
            long start = System.currentTimeMillis();
            while (true) {
                List<String> rows;
                try {
                    rows = TestValuesTableFactory.getRawResultsAsStrings("sink");
                } catch (IllegalArgumentException e) {
                    rows = java.util.Collections.emptyList();
                }

                if (rawResultsReady.test(rows)
                        && "products_ca".equals(materializedChildTable.get())) {
                    return;
                }

                if (System.currentTimeMillis() - start > timeout.toMillis()) {
                    throw new AssertionError(
                            String.format(
                                    "Timeout waiting for PG10 accepted runtime child routing to materialize into snapshotted StreamSplit state for database %s; observed child=%s, rows=%s",
                                    databaseName, materializedChildTable.get(), rows));
                }
                Thread.sleep(interval.toMillis());
            }
        } finally {
            PostgresSourceReader.clearSnapshotStreamSplitHook();
        }
    }

    private void assertParentMetadataRoutingOnly(
            List<String> rows, String... forbiddenChildTables) {
        Assertions.assertThat(rows)
                .allSatisfy(
                        row -> {
                            Assertions.assertThat(row).contains("inventory_partitioned, products");
                            for (String forbiddenChildTable : forbiddenChildTables) {
                                Assertions.assertThat(row).doesNotContain(forbiddenChildTable);
                            }
                        });
    }
}
