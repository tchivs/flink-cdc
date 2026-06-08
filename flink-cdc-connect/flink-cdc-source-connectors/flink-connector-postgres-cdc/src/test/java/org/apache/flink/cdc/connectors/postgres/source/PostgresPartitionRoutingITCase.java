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

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.debezium.connector.postgresql.connection.Lsn;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Integration tests for PostgreSQL partition routing.
 *
 * <p>Verifies that when {@code scan.include-partitioned-tables.enabled = true}, DML events on child
 * partitions are correctly routed to the parent table, and both {@code initial} and {@code
 * latest-offset} startup modes work end-to-end.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class PostgresPartitionRoutingITCase extends PostgresTestBase {

    private static final String SCHEMA_NAME = "inventory_partitioned";
    private static final int DEFAULT_PARALLELISM = 2;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    private final UniqueDatabase partitionedDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres3",
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    public void before() {
        partitionedDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        Thread.sleep(1000L);
        partitionedDatabase.removeSlot(slotName);
    }

    // ==================== initial mode tests ====================

    /**
     * Tests that snapshot reads all existing rows from child partitions, reported under the parent
     * table, followed by streaming changes.
     */
    @Test
    void testPartitionRoutingWithInitialMode() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(buildSourceDDL("initial"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        // Step 1: Verify snapshot data (9 rows from inventory_partitioned.sql)
        List<String> expectedSnapshot =
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

        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> snapshotResults = fetchRows(iterator, expectedSnapshot.size());
        assertEqualsInAnyOrder(expectedSnapshot, snapshotResults);

        // Step 2: Make streaming changes on child partitions, verify routing to parent
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);

            // INSERT into products_us (child partition)
            stat.execute(
                    "INSERT INTO products VALUES (110, 'desk lamp', 'LED desk lamp', 2.5, 'us')");
            // UPDATE a row in products_uk (child partition)
            stat.execute(
                    "UPDATE products SET description = '18oz carpenter hammer' WHERE id = 106 AND country = 'uk'");
            // DELETE a row from products_us (child partition)
            stat.execute("DELETE FROM products WHERE id = 101 AND country = 'us'");
        }

        List<String> expectedStream =
                Arrays.asList(
                        "+I[110, desk lamp, LED desk lamp, 2.5, us]",
                        "-U[106, hammer, 16oz carpenter's hammer, 1.0, uk]",
                        "+U[106, hammer, 18oz carpenter hammer, 1.0, uk]",
                        "-D[101, scooter, Small 2-wheel scooter, 3.14, us]");

        List<String> streamResults = fetchRows(iterator, expectedStream.size());
        assertEqualsInAnyOrder(expectedStream, streamResults);

        // Cleanup
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

    // ==================== latest-offset mode tests ====================

    /**
     * Tests that latest-offset mode skips snapshot and only captures streaming DML changes on child
     * partitions, correctly routed to the parent table.
     */
    @Test
    void testPartitionRoutingWithLatestOffsetMode() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(buildSourceDDL("latest-offset"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        // Wait for the source to be running in streaming mode
        waitForJobRunning(tableResult);

        // Make DML changes on child partitions
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);

            // INSERT into different child partitions
            stat.execute(
                    "INSERT INTO products VALUES (110, 'backpack', 'waterproof backpack', 3.0, 'us')");
            stat.execute(
                    "INSERT INTO products VALUES (111, 'umbrella', 'foldable umbrella', 0.5, 'uk')");
            // UPDATE across child partitions
            stat.execute("UPDATE products SET weight = 9.0 WHERE id = 102 AND country = 'us'");
            // DELETE from a child partition
            stat.execute("DELETE FROM products WHERE id = 109 AND country = 'uk'");
        }

        List<String> expectedStream =
                Arrays.asList(
                        "+I[110, backpack, waterproof backpack, 3.0, us]",
                        "+I[111, umbrella, foldable umbrella, 0.5, uk]",
                        "-U[102, car battery, 12V car battery, 8.1, us]",
                        "+U[102, car battery, 12V car battery, 9.0, us]",
                        "-D[109, spare tire, 24 inch spare tire, 22.2, uk]");

        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> streamResults = fetchRows(iterator, expectedStream.size());
        assertEqualsInAnyOrder(expectedStream, streamResults);

        // Cleanup
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

    // ==================== new partition discovery test ====================

    /**
     * Tests that a new child partition created at runtime is discovered and its events are
     * correctly routed to the parent table.
     */
    @Test
    void testNewPartitionDiscoveryDuringStreaming() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(buildSourceDDL("latest-offset"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        waitForJobRunning(tableResult);

        // Create a new child partition at runtime and insert data
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);

            // Create new child partition
            stat.execute("CREATE TABLE products_de PARTITION OF products FOR VALUES IN ('de')");
            stat.execute("ALTER TABLE products_de REPLICA IDENTITY FULL");
            // Insert into existing child first
            stat.execute("INSERT INTO products VALUES (112, 'pen', 'ballpoint pen', 0.1, 'us')");
            // Insert into the new child partition
            stat.execute(
                    "INSERT INTO products VALUES (113, 'bier', 'German wheat beer', 1.2, 'de')");
        }

        List<String> expectedStream =
                Arrays.asList(
                        "+I[112, pen, ballpoint pen, 0.1, us]",
                        "+I[113, bier, German wheat beer, 1.2, de]");

        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> results = fetchRows(iterator, expectedStream.size());
        // Asserting "in any order" without a defensive "no exception thrown" check is risky
        // here: the new-partition path previously failed in the WAL thread with NPEs (child
        // schema not registered) which would surface as a job restart, not as missing rows.
        // Re-throwing job failures inside fetchRows is what protects the assertion below from
        // silently passing on a broken pipeline; this comment documents the contract.
        assertEqualsInAnyOrder(expectedStream, results);

        // Cleanup
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

    @Test
    void testNewPartitionDiscoveryWithFilteredPublication() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(
                buildSourceDDL(
                        "latest-offset",
                        ","
                                + " 'scan.partition-discovery.poll-interval' = '1s',"
                                + " 'debezium.publication.autocreate.mode' = 'filtered'"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        waitForJobRunning(tableResult);

        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);
            stat.execute("CREATE TABLE products_de PARTITION OF products FOR VALUES IN ('de')");
            stat.execute("ALTER TABLE products_de REPLICA IDENTITY FULL");
        }

        // Wait until the catalog poller adds the new child to the FILTERED publication.
        waitUntilPublished("dbz_publication", "products_de");

        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);
            stat.execute(
                    "INSERT INTO products VALUES (113, 'bier', 'German wheat beer', 1.2, 'de')");
        }

        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> results = fetchRows(iterator, 1);
        assertEqualsInAnyOrder(Arrays.asList("+I[113, bier, German wheat beer, 1.2, de]"), results);

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

    @Test
    void testPartitionRoutingStateRebuiltAfterCheckpointRecovery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 0);

        tEnv.executeSql(buildSourceDDL("latest-offset"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        waitForJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        JobClient jobClient = tableResult.getJobClient().get();
        JobID jobId = jobClient.getJobID();

        try {
            try (Connection conn =
                            getJdbcConnection(
                                    POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                    Statement stat = conn.createStatement()) {
                stat.execute("SET search_path TO " + SCHEMA_NAME);
                stat.execute(
                        "INSERT INTO products VALUES (112, 'pen', 'ballpoint pen', 0.1, 'us')");
            }

            assertEqualsInAnyOrder(
                    Arrays.asList("+I[112, pen, ballpoint pen, 0.1, us]"), fetchRows(iterator, 1));

            miniClusterResource.get().getMiniCluster().triggerCheckpoint(jobId).get();
            createGermanPartition();

            PostgresTestUtils.triggerFailover(
                    PostgresTestUtils.FailoverType.TM,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> {});
            PostgresTestUtils.waitUntilJobRunning(tableResult);

            try (Connection conn =
                            getJdbcConnection(
                                    POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                    Statement stat = conn.createStatement()) {
                stat.execute("SET search_path TO " + SCHEMA_NAME);
                stat.execute(
                        "INSERT INTO products VALUES (113, 'bier', 'German wheat beer', 1.2, 'de')");
            }

            assertEqualsInAnyOrder(
                    Arrays.asList("+I[113, bier, German wheat beer, 1.2, de]"),
                    fetchRows(iterator, 1));
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testMixedPartitionedAndRegularTablesConsumeAndAdvanceWal() throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);
            stat.execute(
                    "CREATE TABLE regular_users ("
                            + "id INT PRIMARY KEY, "
                            + "name VARCHAR(255) NOT NULL, "
                            + "description VARCHAR(512), "
                            + "weight FLOAT, "
                            + "country VARCHAR(20) NOT NULL)");
            stat.execute("ALTER TABLE regular_users REPLICA IDENTITY FULL");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(
                buildSourceDDL(
                        "latest-offset",
                        "products,regular_users",
                        "," + " 'debezium.publication.autocreate.mode' = 'filtered'",
                        0));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");
        waitForJobRunning(tableResult);

        CloseableIterator<Row> iterator = tableResult.collect();
        JobClient jobClient = tableResult.getJobClient().get();
        JobID jobId = jobClient.getJobID();

        try {
            waitUntilSlotReady();
            String flushLsnBefore = getConfirmedFlushLsn();

            try (Connection conn =
                            getJdbcConnection(
                                    POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                    Statement stat = conn.createStatement()) {
                stat.execute("SET search_path TO " + SCHEMA_NAME);
                stat.execute(
                        "INSERT INTO regular_users VALUES "
                                + "(901, 'alice', 'regular table event', 1.5, 'cn')");
                stat.execute(
                        "INSERT INTO products VALUES "
                                + "(902, 'desk', 'partition table event', 2.5, 'us')");
            }

            assertEqualsInAnyOrder(
                    Arrays.asList(
                            "+I[901, alice, regular table event, 1.5, cn]",
                            "+I[902, desk, partition table event, 2.5, us]"),
                    fetchRows(iterator, 2));

            miniClusterResource.get().getMiniCluster().triggerCheckpoint(jobId).get();
            waitUntilConfirmedFlushLsnAdvancedBeyond(flushLsnBefore);
        } finally {
            iterator.close();
            jobClient.cancel().get();
        }
    }

    /**
     * Regression test for the partitioned-parent publication BUG.
     *
     * <p>When {@code scan.include-partitioned-tables.enabled=true} and {@code
     * publication.autocreate.mode=filtered}, Debezium's {@code
     * PostgresReplicationConnection.determineCapturedTables()} previously included partitioned
     * parent tables in the {@code CREATE/ALTER PUBLICATION ... FOR TABLE} statement. PostgreSQL
     * 10/11/12 reject this with:
     *
     * <pre>
     *   ERROR: "&lt;parent&gt;" is a partitioned table
     *   Detail: Adding partitioned tables to publications is not supported.
     * </pre>
     *
     * <p>PostgreSQL 13+ silently accepts the statement, which would still bypass the Flink-CDC
     * dispatcher's child-to-parent routing path. This test asserts the publication membership
     * directly so it catches both regressions regardless of the server version (the test container
     * is currently PG 14, but the same assertion holds on PG 10/11/12).
     */
    @Test
    void testFilteredPublicationExcludesPartitionedParents() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0);

        tEnv.executeSql(
                buildSourceDDL(
                        "latest-offset",
                        "," + " 'debezium.publication.autocreate.mode' = 'filtered'"));
        TableResult tableResult = tEnv.executeSql("SELECT * FROM partitioned_products");

        waitForJobRunning(tableResult);

        // Wait until both pre-existing children are added to the publication. This implicitly
        // verifies the "CREATE PUBLICATION ... FOR TABLE" statement succeeded — on PG 10/11/12
        // the unfiltered version of this statement (which contained the parent 'products')
        // would have failed before reaching this point.
        waitUntilPublished("dbz_publication", "products_us");
        waitUntilPublished("dbz_publication", "products_uk");

        // Core assertion: the partitioned parent must not be a member of the publication,
        // even on PostgreSQL 13+ where the server would accept it.
        Assertions.assertThat(isPublished("dbz_publication", "products"))
                .as(
                        "partitioned parent 'products' must NOT be a publication member; "
                                + "PostgreSQL 10/11/12 reject such publications and PG 13+ would "
                                + "bypass Flink-CDC's child-to-parent routing dispatcher.")
                .isFalse();

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

    // ==================== helpers ====================

    private String buildSourceDDL(String scanStartupMode) {
        return buildSourceDDL(scanStartupMode, "");
    }

    private String buildSourceDDL(String scanStartupMode, String extraOptions) {
        return buildSourceDDL(scanStartupMode, "products", extraOptions, 1);
    }

    private String buildSourceDDL(
            String scanStartupMode, String tableNamePattern, String extraOptions) {
        return buildSourceDDL(scanStartupMode, tableNamePattern, extraOptions, 1);
    }

    private String buildSourceDDL(
            String scanStartupMode, String tableNamePattern, String extraOptions, int lsnDelay) {
        return format(
                "CREATE TABLE partitioned_products ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight FLOAT,"
                        + " country STRING,"
                        + " PRIMARY KEY (id, country) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '100',"
                        + " 'decoding.plugin.name' = 'pgoutput',"
                        + " 'slot.name' = '%s',"
                        + " 'scan.include-partitioned-tables.enabled' = 'true',"
                        + " 'scan.lsn-commit.checkpoints-num-delay' = '%s'"
                        + "%s"
                        + ")",
                partitionedDatabase.getHost(),
                partitionedDatabase.getDatabasePort(),
                partitionedDatabase.getUsername(),
                partitionedDatabase.getPassword(),
                partitionedDatabase.getDatabaseName(),
                SCHEMA_NAME,
                tableNamePattern,
                scanStartupMode,
                slotName,
                lsnDelay,
                extraOptions);
    }

    private void waitForJobRunning(TableResult tableResult) throws InterruptedException {
        // Give the source time to initialize and start streaming
        Thread.sleep(5000L);
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
                        "Timed out waiting for %s.%s to be added to publication %s",
                        SCHEMA_NAME, tableName, publicationName));
    }

    private boolean isPublished(String publicationName, String tableName) throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
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

    private void waitUntilSlotReady() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            String confirmedFlushLsn = getConfirmedFlushLsn();
            if (confirmedFlushLsn != null && !"0/0".equals(confirmedFlushLsn)) {
                return;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError("Timed out waiting for replication slot " + slotName);
    }

    private void waitUntilConfirmedFlushLsnAdvancedBeyond(String previousLsn) throws Exception {
        long previous = Lsn.valueOf(previousLsn).asLong();
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            String currentLsn = getConfirmedFlushLsn();
            if (currentLsn != null && Lsn.valueOf(currentLsn).asLong() > previous) {
                return;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError(
                format(
                        "Timed out waiting for confirmed_flush_lsn of slot %s to advance beyond %s; current=%s",
                        slotName, previousLsn, getConfirmedFlushLsn()));
    }

    private String getConfirmedFlushLsn() throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement();
                ResultSet rs =
                        stat.executeQuery(
                                format(
                                        "SELECT confirmed_flush_lsn FROM pg_replication_slots "
                                                + "WHERE slot_name = '%s'",
                                        slotName))) {
            if (!rs.next()) {
                return null;
            }
            return rs.getString(1);
        }
    }

    private void createGermanPartition() throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, partitionedDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute("SET search_path TO " + SCHEMA_NAME);
            stat.execute("CREATE TABLE products_de PARTITION OF products FOR VALUES IN ('de')");
            stat.execute("ALTER TABLE products_de REPLICA IDENTITY FULL");
        }
    }

    /**
     * Collects rows from the iterator with a timeout. This method blocks until the desired number
     * of rows are collected or a timeout is reached.
     */
    public static List<String> fetchRows(CloseableIterator<Row> iterator, int size) {
        List<String> rows = new ArrayList<>(size);
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread thread = new Thread(r, "postgres-partition-result-fetcher");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            while (rows.size() < size) {
                FutureTask<Row> future = new FutureTask<>(iterator::next);
                executor.execute(future);
                rows.add(future.get(60, TimeUnit.SECONDS).toString());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AssertionError("Failed to fetch result rows", e.getCause());
        } catch (TimeoutException e) {
            throw new AssertionError(
                    format("Expected %d rows but only got %d within timeout", size, rows.size()),
                    e);
        } finally {
            executor.shutdownNow();
        }
        return rows;
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        List<String> sortedExpected = expected.stream().sorted().collect(Collectors.toList());
        List<String> sortedActual = actual.stream().sorted().collect(Collectors.toList());
        Assertions.assertThat(sortedActual).isEqualTo(sortedExpected);
    }
}
