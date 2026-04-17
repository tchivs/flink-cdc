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

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
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
import java.util.function.Predicate;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for PostgreSQL to start from a savepoint. */
class PostgreSQLSavepointITCase extends PostgresTestBase {

    @TempDir private Path tempDir;

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
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
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient
                        .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
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
}
