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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL 10 has no {@code publish_via_partition_root}: only the partition root is configured
 * and its partitions are routed on the client. A partition swapped in while the job runs (DETACH
 * the old October partition, ATTACH a new one under a new name) must still be captured as the
 * parent table, without a child table identity leaking downstream.
 */
class PostgresPartitionSwapITCase extends AbstractTestBase {

    private static final String DATABASE = "pg10route";

    private static final PostgreSQLContainer<?> PG10 =
            new PostgreSQLContainer<>(
                            DockerImageName.parse("debezium/postgres:10")
                                    .asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName(DATABASE)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withCommand(
                            "postgres",
                            "-c",
                            "fsync=off",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "max_wal_senders=20");

    private static final TableId PARENT = TableId.tableId(DATABASE, "public", "jjdb");

    @BeforeAll
    static void startContainer() {
        PG10.start();
    }

    @AfterAll
    static void stopContainer() {
        PG10.stop();
    }

    @Test
    void partitionSwappedWhileRunningIsCapturedAsParent() throws Exception {
        execute(
                "CREATE TABLE public.jjdb (id INT NOT NULL, dt DATE NOT NULL, name VARCHAR(32))"
                        + " PARTITION BY RANGE (dt)",
                "CREATE TABLE public.jjdb_202609 PARTITION OF public.jjdb"
                        + " FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')",
                "ALTER TABLE public.jjdb_202609 ADD PRIMARY KEY (id, dt)",
                "CREATE TABLE public.\"121211_jjdb_202610_111\" PARTITION OF public.jjdb"
                        + " FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')",
                "ALTER TABLE public.\"121211_jjdb_202610_111\" ADD PRIMARY KEY (id, dt)",
                // As provisioned by the DBA in production.
                "CREATE PUBLICATION dbz_publication FOR ALL TABLES",
                "INSERT INTO public.jjdb VALUES (1, '2026-09-15', 'sep'), (2, '2026-10-02', 'oct')");

        Configuration config = new Configuration();
        config.set(PostgresDataSourceOptions.HOSTNAME, PG10.getHost());
        config.set(
                PostgresDataSourceOptions.PG_PORT,
                PG10.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        config.set(PostgresDataSourceOptions.USERNAME, PG10.getUsername());
        config.set(PostgresDataSourceOptions.PASSWORD, PG10.getPassword());
        config.set(PostgresDataSourceOptions.SLOT_NAME, "flink_partition_swap");
        config.set(PostgresDataSourceOptions.DECODING_PLUGIN_NAME, "pgoutput");
        config.set(PostgresDataSourceOptions.TABLES, DATABASE + ".public.jjdb");
        config.set(PostgresDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        config.set(PostgresDataSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED, true);
        config.set(PostgresDataSourceOptions.TABLE_ID_INCLUDE_DATABASE, true);
        config.set(PostgresDataSourceOptions.SCHEMA_CHANGE_ENABLED, true);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSourceFactory()
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                config,
                                                new Configuration(),
                                                getClass().getClassLoader()))
                                .getEventSourceProvider();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500L);
        // The unrouted partition fails the source over exactly once.
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 1000L);

        List<Event> events = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> jobFailure = new AtomicReference<>();
        try (CloseableIterator<Event> iterator =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect()) {
            Thread collector =
                    new Thread(
                            () -> {
                                try {
                                    while (iterator.hasNext()) {
                                        events.add(iterator.next());
                                    }
                                } catch (Throwable t) {
                                    jobFailure.set(t);
                                }
                            });
            collector.setDaemon(true);
            collector.start();

            awaitParentRows(events, jobFailure, 1, 2);
            execute("INSERT INTO public.jjdb VALUES (3, '2026-10-03', 'oct-old')");
            awaitParentRows(events, jobFailure, 3);
            // Let a checkpoint cover the old partition before it goes away.
            Thread.sleep(3000L);

            execute(
                    "ALTER TABLE public.jjdb DETACH PARTITION public.\"121211_jjdb_202610_111\"",
                    "CREATE TABLE public.jjdb_202610 (id INT NOT NULL, dt DATE NOT NULL,"
                            + " name VARCHAR(32), PRIMARY KEY (id, dt))",
                    "ALTER TABLE public.jjdb ATTACH PARTITION public.jjdb_202610"
                            + " FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')",
                    "INSERT INTO public.jjdb VALUES (10, '2026-10-10', 'oct-new'),"
                            + " (11, '2026-10-11', 'oct-new')");
            awaitParentRows(events, jobFailure, 10, 11);
        }

        List<CreateTableEvent> createTableEvents =
                events.stream()
                        .filter(CreateTableEvent.class::isInstance)
                        .map(CreateTableEvent.class::cast)
                        .collect(Collectors.toList());
        assertThat(createTableEvents).isNotEmpty();
        assertThat(createTableEvents).allSatisfy(e -> assertThat(e.tableId()).isEqualTo(PARENT));
        assertThat(createTableEvents.get(0).getSchema().primaryKeys()).containsExactly("id", "dt");
        assertThat(events)
                .filteredOn(DataChangeEvent.class::isInstance)
                .allSatisfy(e -> assertThat(((DataChangeEvent) e).tableId()).isEqualTo(PARENT));
    }

    private static void awaitParentRows(
            List<Event> events, AtomicReference<Throwable> jobFailure, Integer... ids)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000L;
        while (!parentRowIds(events).containsAll(Arrays.asList(ids))) {
            if (jobFailure.get() != null) {
                throw new AssertionError("job failed", jobFailure.get());
            }
            assertThat(System.currentTimeMillis())
                    .as(
                            "rows %s of %s within 120s, received: %s",
                            Arrays.toString(ids), PARENT, events)
                    .isLessThan(deadline);
            Thread.sleep(200L);
        }
    }

    private static Set<Integer> parentRowIds(List<Event> events) {
        return events.stream()
                .filter(DataChangeEvent.class::isInstance)
                .map(DataChangeEvent.class::cast)
                .filter(e -> e.tableId().equals(PARENT) && e.after() != null)
                .map(e -> e.after().getInt(0))
                .collect(Collectors.toSet());
    }

    private static void execute(String... statements) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword());
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
