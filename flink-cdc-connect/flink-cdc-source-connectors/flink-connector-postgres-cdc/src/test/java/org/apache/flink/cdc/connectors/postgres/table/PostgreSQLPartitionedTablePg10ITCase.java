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
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** PG10 IT case for partition child table routing to parent table id. */
class PostgreSQLPartitionedTablePg10ITCase extends PostgresPg10TestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Test
    void testRoutesChildPartitionEventsToParentTable() throws Exception {
        TestValuesTableFactory.clearAllData();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        initializePg10PartitionedTable();
        String publicationName = "dbz_publication_pg10_" + new Random().nextInt(1000);
        String slotName = getSlotName();

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us",
                            publicationName));
            statement.execute(
                    String.format(
                            "select pg_create_logical_replication_slot('%s','pgoutput');",
                            slotName));
        }

        String sourceDDL =
                String.format(
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
                                + " 'decoding.plugin.name' = 'pgoutput',"
                                + " 'debezium.publication.name' = '%s',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory_partitioned",
                        "products",
                        publicationName,
                        slotName);

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        // wait for source startup and replication slot readiness
        Thread.sleep(10000L);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) "
                            + "VALUES ('helmet', 'bike helmet', 0.50, 'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) "
                            + "VALUES ('gloves', 'winter gloves', 0.20, 'us');");
        }

        waitForCondition(
                "child partition DML routed to parent",
                rows ->
                        rows.stream().anyMatch(row -> row.contains("helmet"))
                                && rows.stream().anyMatch(row -> row.contains("gloves")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual)
                .anySatisfy(row -> Assertions.assertThat(row).contains("helmet"));
        Assertions.assertThat(actual)
                .anySatisfy(row -> Assertions.assertThat(row).contains("gloves"));
        Assertions.assertThat(actual)
                .allSatisfy(
                        row -> {
                            Assertions.assertThat(row).contains("inventory_partitioned, products");
                            Assertions.assertThat(row).doesNotContain("products_uk");
                            Assertions.assertThat(row).doesNotContain("products_us");
                        });

        result.getJobClient().get().cancel().get();
    }

    @Test
    void testConsumingAllEventsForPartitionedTablePg10() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_all_events");
        initializePg10PartitionedTable(databaseName);

        String publicationName = "dbz_publication_pg10_full_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "pgoutput",
                        publicationName,
                        slotName,
                        null);

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        waitForCondition(
                "partitioned table snapshot started",
                rows ->
                        rows.stream().anyMatch(row -> row.contains("scooter"))
                                || rows.stream().anyMatch(row -> row.contains("hammer")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        Thread.sleep(5000L);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) "
                            + "VALUES ('helmet', 'bike helmet', 0.50, 'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) "
                            + "VALUES ('gloves', 'winter gloves', 0.20, 'us');");
        }

        waitForCondition(
                "snapshot + streaming records consumed and routed to parent",
                rows ->
                        rows.stream().anyMatch(row -> row.contains("helmet"))
                                && rows.stream().anyMatch(row -> row.contains("gloves")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual)
                .anySatisfy(
                        row ->
                                Assertions.assertThat(row)
                                        .containsAnyOf("scooter", "hammer", "rocks", "jacket"))
                .anySatisfy(row -> Assertions.assertThat(row).contains("helmet"))
                .anySatisfy(row -> Assertions.assertThat(row).contains("gloves"));
        Assertions.assertThat(actual)
                .allSatisfy(
                        row -> {
                            Assertions.assertThat(row).contains("inventory_partitioned, products");
                            Assertions.assertThat(row).doesNotContain("products_uk");
                            Assertions.assertThat(row).doesNotContain("products_us");
                        });

        result.getJobClient().get().cancel().get();
    }

    @Test
    void testRejectsPublicationMissingChildPartition() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_missing_child");
        initializePg10PartitionedTable(databaseName);

        String publicationName = "dbz_publication_pg10_partial_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName, publicationName, slotName, "inventory_partitioned.products_uk");

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "pgoutput",
                        publicationName,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        Assertions.assertThatThrownBy(result::await)
                .hasStackTraceContaining("PG10 partition publication validation failed")
                .hasStackTraceContaining("products_us")
                .hasStackTraceContaining(publicationName);
    }

    @Test
    void testFallbackRestartChangesReplicationSlotPidForRuntimeChildPg10() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_runtime_restart");
        initializePg10PartitionedTable(databaseName);

        String publicationName =
                "dbz_publication_pg10_runtime_restart_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "pgoutput",
                        publicationName,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        try {
            long initialActivePid =
                    waitForReplicationSlotActivePid(
                            databaseName, slotName, Duration.ofMinutes(2), Duration.ofSeconds(1));

            String existingChildMarker = "helmet_restart_direct_" + new Random().nextInt(1000000);
            String runtimeChildMarker = "snowboard_restart_direct_" + new Random().nextInt(1000000);

            insertPartitionRow(
                    databaseName,
                    "products_uk",
                    existingChildMarker,
                    "baseline_before_runtime_restart",
                    0.50,
                    "uk");

            waitForCondition(
                    "existing PG10 child partition captured before runtime restart",
                    rows -> countRowsContaining(rows, existingChildMarker) == 1,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1));

            createRuntimePartition(databaseName, "products_ca", "ca");

            // Keep this comfortably above the 30s publication poll cycle so startup jitter and
            // JDBC execution still leave enough time for the runtime child to be added to the
            // publication and trigger the fallback restart.
            waitForPublicationTable(
                    databaseName,
                    publicationName,
                    "products_ca",
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1));

            long restartedActivePid =
                    waitForReplicationSlotActivePidChange(
                            databaseName,
                            slotName,
                            initialActivePid,
                            Duration.ofMinutes(2),
                            Duration.ofSeconds(1));
            Assertions.assertThat(restartedActivePid).isNotEqualTo(initialActivePid);

            insertPartitionRow(
                    databaseName,
                    "products_ca",
                    runtimeChildMarker,
                    "captured_after_runtime_restart",
                    3.40,
                    "ca");

            waitForCondition(
                    "runtime-created PG10 child partition captured after fallback restart",
                    rows -> countRowsContaining(rows, runtimeChildMarker) == 1,
                    Duration.ofSeconds(45),
                    Duration.ofSeconds(1));

            java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
            Assertions.assertThat(countRowsContaining(actual, existingChildMarker)).isEqualTo(1L);
            Assertions.assertThat(countRowsContaining(actual, runtimeChildMarker)).isEqualTo(1L);
            assertParentMetadataRoutingOnly(actual, "products_uk", "products_us", "products_ca");
        } finally {
            cancelJob(result);
        }
    }

    @Test
    void testPartitionedTableStartupFromLatestOffsetCapturesOnlyPostStartupPartitionChanges()
            throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_latest_post_startup_only");
        initializePg10PartitionedTable(databaseName);

        String publicationName = "dbz_publication_pg10_latest_only_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) "
                            + "VALUES ('history_uk', 'before startup', 0.10, 'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) "
                            + "VALUES ('history_us', 'before startup', 0.20, 'us');");
        }

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "pgoutput",
                        publicationName,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        Thread.sleep(10000L);

        java.util.List<String> startupRows = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(startupRows)
                .noneMatch(row -> row.contains("history_uk"))
                .noneMatch(row -> row.contains("history_us"));

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) "
                            + "VALUES ('helmet', 'bike helmet', 0.50, 'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) "
                            + "VALUES ('gloves', 'winter gloves', 0.20, 'us');");
        }

        waitForCondition(
                "post-startup partition changes captured with latest-offset",
                rows ->
                        rows.stream().anyMatch(row -> row.contains("helmet"))
                                && rows.stream().anyMatch(row -> row.contains("gloves")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual)
                .noneMatch(row -> row.contains("history_uk"))
                .noneMatch(row -> row.contains("history_us"))
                .anySatisfy(row -> Assertions.assertThat(row).contains("helmet"))
                .anySatisfy(row -> Assertions.assertThat(row).contains("gloves"));
        Assertions.assertThat(actual)
                .allSatisfy(
                        row -> {
                            Assertions.assertThat(row).contains("inventory_partitioned, products");
                            Assertions.assertThat(row).doesNotContain("products_uk");
                            Assertions.assertThat(row).doesNotContain("products_us");
                        });

        result.getJobClient().get().cancel().get();
    }

    @Test
    void testRestartResumesFromLastCommitBoundaryForLongTransactionPg10() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_long_tx_boundary");
        initializePg10PartitionedTable(databaseName);

        String publicationName = "dbz_publication_pg10_boundary_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "pgoutput",
                        publicationName,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        try {
            long initialActivePid =
                    waitForReplicationSlotActivePid(
                            databaseName, slotName, Duration.ofMinutes(2), Duration.ofSeconds(1));

            String committedBeforeRestartMarker =
                    "before_restart_marker_" + new Random().nextInt(1000000);
            String longTransactionMarker =
                    "boundary_commit_marker_" + new Random().nextInt(1000000);
            String runtimeChildAfterRestartMarker =
                    "ca_after_restart_marker_" + new Random().nextInt(1000000);

            insertPartitionRow(
                    databaseName,
                    "products_uk",
                    committedBeforeRestartMarker,
                    "committed_before_restart",
                    0.10,
                    "uk");

            waitForCondition(
                    "committed marker captured before restart boundary",
                    rows -> countRowsContaining(rows, committedBeforeRestartMarker) == 1,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1));

            createRuntimePartition(databaseName, "products_ca", "ca");

            try (Connection longTransactionConnection =
                            getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                    Statement longTransactionStatement =
                            longTransactionConnection.createStatement()) {
                longTransactionConnection.setAutoCommit(false);
                longTransactionStatement.execute(
                        buildInsertPartitionSql(
                                "products_uk",
                                longTransactionMarker,
                                "commit_after_restart",
                                0.11,
                                "uk"));

                waitForPublicationTable(
                        databaseName,
                        publicationName,
                        "products_ca",
                        Duration.ofMinutes(2),
                        Duration.ofSeconds(1));

                long restartedActivePid =
                        waitForReplicationSlotActivePidChange(
                                databaseName,
                                slotName,
                                initialActivePid,
                                Duration.ofMinutes(2),
                                Duration.ofSeconds(1));
                Assertions.assertThat(restartedActivePid).isNotEqualTo(initialActivePid);

                java.util.List<String> rowsBeforeCommit =
                        TestValuesTableFactory.getRawResultsAsStrings("sink");
                Assertions.assertThat(
                                countRowsContaining(rowsBeforeCommit, committedBeforeRestartMarker))
                        .isEqualTo(1L);
                Assertions.assertThat(rowsBeforeCommit)
                        .noneMatch(row -> row.contains(longTransactionMarker));

                longTransactionConnection.commit();
            }

            waitForCondition(
                    "long transaction committed exactly once after restart boundary",
                    rows -> countRowsContaining(rows, longTransactionMarker) == 1,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1));

            java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
            Assertions.assertThat(countRowsContaining(actual, committedBeforeRestartMarker))
                    .isEqualTo(1L);
            Assertions.assertThat(countRowsContaining(actual, longTransactionMarker)).isEqualTo(1L);
            assertParentMetadataRoutingOnly(actual, "products_uk", "products_us", "products_ca");
        } finally {
            cancelJob(result);
        }
    }

    @Test
    void testRejectsPluginMismatch() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_plugin_mismatch");
        initializePg10PartitionedTable(databaseName);

        String slotName = getSlotName();
        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products",
                        true,
                        "decoderbufs",
                        null,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        Assertions.assertThatThrownBy(result::await).hasStackTraceContaining("decoderbufs");
    }

    @Test
    void testDisabledPartitionTablesBehavior() throws Exception {
        setup();

        String databaseName = createUniqueDatabase("pg10_partitioned_disabled_behavior");
        initializePg10PartitionedTable(databaseName);

        String publicationName = "dbz_publication_pg10_disabled_" + new Random().nextInt(1000);
        String slotName = getSlotName();
        createPublicationAndSlot(
                databaseName,
                publicationName,
                slotName,
                "inventory_partitioned.products_uk, inventory_partitioned.products_us");

        String sourceDDL =
                createSourceDDL(
                        databaseName,
                        "products_(uk|us)",
                        false,
                        "pgoutput",
                        publicationName,
                        slotName,
                        "latest-offset");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT schema_name, table_name, id, name, country FROM debezium_source");

        Thread.sleep(10000L);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) "
                            + "VALUES ('helmet', 'bike helmet', 0.50, 'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) "
                            + "VALUES ('gloves', 'winter gloves', 0.20, 'us');");
        }

        waitForCondition(
                "explicit child tables captured without parent routing",
                rows ->
                        rows.stream()
                                        .anyMatch(
                                                row ->
                                                        row.contains("products_uk")
                                                                && row.contains("helmet"))
                                && rows.stream()
                                        .anyMatch(
                                                row ->
                                                        row.contains("products_us")
                                                                && row.contains("gloves")),
                Duration.ofMinutes(2),
                Duration.ofSeconds(1));

        java.util.List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual)
                .anySatisfy(row -> Assertions.assertThat(row).contains("products_uk"))
                .anySatisfy(row -> Assertions.assertThat(row).contains("products_us"));
        Assertions.assertThat(actual)
                .allSatisfy(
                        row ->
                                Assertions.assertThat(row)
                                        .doesNotContain("inventory_partitioned, products,"));

        result.getJobClient().get().cancel().get();
    }

    private void initializePg10PartitionedTable() {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializePg10PartitionedTable(String databaseName) {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setup() {
        TestValuesTableFactory.clearAllData();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
    }

    private String createUniqueDatabase(String logicalName) {
        UniqueDatabase uniqueDatabase =
                new UniqueDatabase(
                        POSTGRES_CONTAINER,
                        logicalName,
                        "inventory_partitioned",
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());

        try (Connection defaultConnection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = defaultConnection.createStatement()) {
            statement.execute("CREATE DATABASE " + uniqueDatabase.getDatabaseName());
            return uniqueDatabase.getDatabaseName();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createPublicationAndSlot(
            String databaseName, String publicationName, String slotName, String publicationTables)
            throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
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

    private String createSourceDDL(
            String databaseName,
            String tableName,
            boolean includePartitionedTables,
            String pluginName,
            String publicationName,
            String slotName,
            String startupMode) {
        String publicationOption =
                publicationName == null
                        ? ""
                        : String.format(", 'debezium.publication.name' = '%s'", publicationName);
        String startupModeOption =
                startupMode == null
                        ? ""
                        : String.format(", 'scan.startup.mode' = '%s'", startupMode);

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
                        + " 'scan.include-partitioned-tables.enabled' = '%s',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'scan.incremental.snapshot.chunk.key-column' = 'id',"
                        + " 'decoding.plugin.name' = '%s',"
                        + " 'slot.name' = '%s'"
                        + "%s"
                        + "%s"
                        + ")",
                POSTGRES_CONTAINER.getHost(),
                POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword(),
                databaseName,
                "inventory_partitioned",
                tableName,
                includePartitionedTables,
                pluginName,
                slotName,
                publicationOption,
                startupModeOption);
    }

    private void createRuntimePartition(String databaseName, String childTableName, String country)
            throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName)) {
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
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    buildInsertPartitionSql(childTableName, name, description, weight, country));
        }
    }

    private String buildInsertPartitionSql(
            String childTableName, String name, String description, double weight, String country) {
        return String.format(
                "INSERT INTO inventory_partitioned.%s (name, description, weight, country) VALUES ('%s', '%s', %s, '%s');",
                childTableName,
                escapeSqlLiteral(name),
                escapeSqlLiteral(description),
                Double.toString(weight),
                escapeSqlLiteral(country));
    }

    private String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private long waitForReplicationSlotActivePid(
            String databaseName, String slotName, Duration timeout, Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Long activePid = getReplicationSlotActivePid(databaseName, slotName);
            if (activePid != null) {
                return activePid;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for replication slot %s to become active",
                                slotName));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private long waitForReplicationSlotActivePidChange(
            String databaseName,
            String slotName,
            long previousActivePid,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Long activePid = getReplicationSlotActivePid(databaseName, slotName);
            if (activePid != null && activePid != previousActivePid) {
                return activePid;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for replication slot %s active_pid to change from %s; latest pid: %s",
                                slotName, previousActivePid, activePid));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private Long getReplicationSlotActivePid(String databaseName, String slotName)
            throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement();
                java.sql.ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '%s' AND database = '%s'",
                                        slotName, databaseName))) {
            if (!resultSet.next()) {
                throw new AssertionError(
                        String.format(
                                "Replication slot %s not found for database %s",
                                slotName, databaseName));
            }
            Object activePid = resultSet.getObject("active_pid");
            return activePid == null ? null : ((Number) activePid).longValue();
        }
    }

    private long countRowsContaining(java.util.List<String> rows, String token) {
        return rows.stream().filter(row -> row.contains(token)).count();
    }

    private void assertParentMetadataRoutingOnly(
            java.util.List<String> rows, String... forbiddenChildTables) {
        Assertions.assertThat(rows)
                .allSatisfy(
                        row -> {
                            Assertions.assertThat(row).contains("inventory_partitioned, products");
                            for (String forbiddenChildTable : forbiddenChildTables) {
                                Assertions.assertThat(row).doesNotContain(forbiddenChildTable);
                            }
                        });
    }

    private void cancelJob(TableResult result) throws Exception {
        result.getJobClient().get().cancel().get(30, TimeUnit.SECONDS);
    }

    private void waitForCondition(
            String conditionName,
            Predicate<java.util.List<String>> condition,
            Duration timeout,
            Duration interval)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            java.util.List<String> rows;
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

    private void waitForPublicationTable(
            String databaseName,
            String publicationName,
            String tableName,
            Duration timeout,
            Duration interval)
            throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            if (publicationContainsTable(databaseName, publicationName, tableName)) {
                return;
            }
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new AssertionError(
                        String.format(
                                "Timeout waiting for publication %s to contain table %s",
                                publicationName, tableName));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    private boolean publicationContainsTable(
            String databaseName, String publicationName, String tableName) throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement();
                java.sql.ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT 1 FROM pg_publication_tables WHERE pubname = '%s' AND schemaname = 'inventory_partitioned' AND tablename = '%s'",
                                        publicationName, tableName))) {
            return resultSet.next();
        }
    }
}
