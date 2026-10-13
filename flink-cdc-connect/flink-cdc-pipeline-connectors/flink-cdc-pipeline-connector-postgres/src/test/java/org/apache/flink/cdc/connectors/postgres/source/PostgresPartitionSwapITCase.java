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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CheckpointedCollectResultBuffer;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIteratorAdapter;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL 10 has no {@code publish_via_partition_root}: only the partition root is configured
 * and its partitions are routed on the client. A partition attached while the job runs, or while it
 * is stopped and then restored from a savepoint or resumed from the slot's committed offset, must
 * still be captured as the parent table, without a child table identity leaking downstream, and a
 * column added to the root must be reported for the root once however many partitions report it.
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

    private static final String OLD_OCTOBER = "public.\"121211_jjdb_202610_111\"";

    /** DETACH the old October partition and ATTACH a new one under a new name. */
    private static final String[] SWAP_OCTOBER = {
        "ALTER TABLE public.jjdb DETACH PARTITION " + OLD_OCTOBER,
        "CREATE TABLE public.jjdb_202610 (id INT NOT NULL, dt DATE NOT NULL,"
                + " name VARCHAR(32), PRIMARY KEY (id, dt))",
        "ALTER TABLE public.jjdb ATTACH PARTITION public.jjdb_202610"
                + " FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')"
    };

    @BeforeAll
    static void startContainer() {
        PG10.start();
    }

    @AfterAll
    static void stopContainer() {
        PG10.stop();
    }

    @ParameterizedTest(name = "table-id.include-database={0}")
    @ValueSource(booleans = {true, false})
    void partitionSwappedWhileRunningIsCapturedAsParent(
            boolean includeDatabase, @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TableId parent = parent(includeDatabase);
        createTables();
        RunningJob job =
                start(
                        sourceProvider(
                                includeDatabase, "flink_partition_swap_" + includeDatabase, null),
                        null);
        try {
            job.awaitParentRows(parent, 1, 2);
            execute("INSERT INTO public.jjdb VALUES (3, '2026-10-03', 'oct-old')");
            job.awaitParentRows(parent, 3);
            // Let a checkpoint cover the old partition before it goes away.
            Thread.sleep(3000L);

            execute(SWAP_OCTOBER);
            execute(
                    "ALTER TABLE public.jjdb ADD COLUMN note VARCHAR(8)",
                    "INSERT INTO public.jjdb VALUES (10, '2026-10-10', 'oct-new', 'n'),"
                            + " (11, '2026-10-11', 'oct-new', 'n')",
                    // The first change of the old partition after the column was added: it is
                    // replayed after the failover, against its checkpointed schema.
                    "INSERT INTO public.jjdb VALUES (4, '2026-09-20', 'sep', 'n')");
            job.awaitParentRows(parent, 10, 11, 4);
            // The unrouted partition fails the source over at most once.
            assertThat(failovers(miniCluster, job)).isLessThanOrEqualTo(1);
        } finally {
            job.close();
        }

        List<CreateTableEvent> createTableEvents = createTableEvents(job.events);
        assertThat(createTableEvents).isNotEmpty();
        assertThat(createTableEvents.get(0).getSchema().primaryKeys()).containsExactly("id", "dt");
        assertOnlyParent(job.events, parent);
        assertThat(job.events)
                .filteredOn(AddColumnEvent.class::isInstance)
                .as("the added column is reported for the root exactly once: %s", job.events)
                .hasSize(1);
    }

    @ParameterizedTest(name = "table-id.include-database={0}")
    @ValueSource(booleans = {true, false})
    void partitionCreatedWhileRunningIsCapturedAsParent(
            boolean includeDatabase, @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TableId parent = parent(includeDatabase);
        createTables();
        RunningJob job =
                start(
                        sourceProvider(
                                includeDatabase, "flink_partition_create_" + includeDatabase, null),
                        null);
        try {
            job.awaitParentRows(parent, 1, 2);
            execute("INSERT INTO public.jjdb VALUES (3, '2026-10-03', 'oct')");
            job.awaitParentRows(parent, 3);
            // Let a checkpoint cover the stream position before the new partition fails over.
            Thread.sleep(3000L);

            // The regular monthly partition.
            execute(
                    "CREATE TABLE public.jjdb_202611 PARTITION OF public.jjdb"
                            + " FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')",
                    "ALTER TABLE public.jjdb_202611 ADD PRIMARY KEY (id, dt)",
                    "INSERT INTO public.jjdb VALUES (30, '2026-11-05', 'nov')");
            job.awaitParentRows(parent, 30);
            assertThat(failovers(miniCluster, job)).isLessThanOrEqualTo(1);
        } finally {
            job.close();
        }
        assertOnlyParent(job.events, parent);
    }

    /**
     * The field restart path: the restored job runs the source created by the client before the
     * swap, so only the config created by each reader knows the new partition.
     */
    @ParameterizedTest(name = "table-id.include-database={0}")
    @ValueSource(booleans = {true, false})
    void partitionSwappedWhileStoppedIsRoutedAfterRestore(
            boolean includeDatabase, @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TableId parent = parent(includeDatabase);
        createTables();
        FlinkSourceProvider sourceProvider =
                sourceProvider(includeDatabase, "flink_partition_restore_" + includeDatabase, null);

        String savepoint;
        RunningJob before = start(sourceProvider, null);
        try {
            before.awaitParentRows(parent, 1, 2);
            execute("INSERT INTO public.jjdb VALUES (3, '2026-10-03', 'oct-old')");
            before.awaitParentRows(parent, 3);
            savepoint = before.stopWithSavepoint(getTempDirPath("savepoints"));
        } finally {
            before.close();
        }

        execute(SWAP_OCTOBER);
        execute(
                "INSERT INTO public.jjdb VALUES (12, '2026-10-12', 'oct-new'),"
                        + " (13, '2026-10-13', 'oct-new'), (14, '2026-09-14', 'sep')");

        RunningJob after = start(sourceProvider, savepoint);
        try {
            after.awaitParentRows(parent, 12, 13, 14);
            assertThat(failovers(miniCluster, after)).isZero();
        } finally {
            after.close();
        }
        assertOnlyParent(after.events, parent);
    }

    /** The field recovery without checkpoint: a fresh job resumes the slot's committed offset. */
    @ParameterizedTest(name = "table-id.include-database={0}")
    @ValueSource(booleans = {true, false})
    void committedOffsetResumesWithoutCheckpoint(
            boolean includeDatabase, @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TableId parent = parent(includeDatabase);
        String slot = "flink_partition_committed_" + includeDatabase;
        createTables();

        RunningJob first = start(sourceProvider(includeDatabase, slot, null), null);
        try {
            first.awaitParentRows(parent, 1, 2);
            String beforeLastInsert = queryString("SELECT pg_current_wal_lsn()::text");
            execute("INSERT INTO public.jjdb VALUES (3, '2026-10-03', 'oct-old')");
            first.awaitParentRows(parent, 3);
            // Completed checkpoints confirm the stream position to the slot.
            awaitSlot(slot, "confirmed_flush_lsn > '" + beforeLastInsert + "'::pg_lsn");
        } finally {
            first.close();
        }
        awaitSlot(slot, "NOT active");

        execute(SWAP_OCTOBER);
        execute(
                "INSERT INTO public.jjdb VALUES (20, '2026-09-20', 'sep'),"
                        + " (21, '2026-10-21', 'oct-new')");

        RunningJob second = start(sourceProvider(includeDatabase, slot, "committed-offset"), null);
        try {
            second.awaitParentRows(parent, 20, 21);
            assertThat(failovers(miniCluster, second)).isZero();
        } finally {
            second.close();
        }
        assertOnlyParent(second.events, parent);
    }

    private static TableId parent(boolean includeDatabase) {
        return includeDatabase
                ? TableId.tableId(DATABASE, "public", "jjdb")
                : TableId.tableId("public", "jjdb");
    }

    private static void createTables() throws SQLException {
        execute(
                "DROP TABLE IF EXISTS public.jjdb, public.jjdb_202610, " + OLD_OCTOBER,
                "DROP PUBLICATION IF EXISTS dbz_publication",
                "CREATE TABLE public.jjdb (id INT NOT NULL, dt DATE NOT NULL, name VARCHAR(32))"
                        + " PARTITION BY RANGE (dt)",
                "CREATE TABLE public.jjdb_202609 PARTITION OF public.jjdb"
                        + " FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')",
                "ALTER TABLE public.jjdb_202609 ADD PRIMARY KEY (id, dt)",
                "CREATE TABLE "
                        + OLD_OCTOBER
                        + " PARTITION OF public.jjdb"
                        + " FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')",
                "ALTER TABLE " + OLD_OCTOBER + " ADD PRIMARY KEY (id, dt)",
                // As provisioned by the DBA in production.
                "CREATE PUBLICATION dbz_publication FOR ALL TABLES",
                "INSERT INTO public.jjdb VALUES (1, '2026-09-15', 'sep'), (2, '2026-10-02', 'oct')");
    }

    private FlinkSourceProvider sourceProvider(
            boolean includeDatabase, String slot, @Nullable String startupMode) {
        Configuration config = new Configuration();
        config.set(PostgresDataSourceOptions.HOSTNAME, PG10.getHost());
        config.set(
                PostgresDataSourceOptions.PG_PORT,
                PG10.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        config.set(PostgresDataSourceOptions.USERNAME, PG10.getUsername());
        config.set(PostgresDataSourceOptions.PASSWORD, PG10.getPassword());
        config.set(PostgresDataSourceOptions.SLOT_NAME, slot);
        config.set(PostgresDataSourceOptions.DECODING_PLUGIN_NAME, "pgoutput");
        config.set(PostgresDataSourceOptions.TABLES, DATABASE + ".public.jjdb");
        config.set(PostgresDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        config.set(PostgresDataSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED, true);
        config.set(PostgresDataSourceOptions.TABLE_ID_INCLUDE_DATABASE, includeDatabase);
        config.set(PostgresDataSourceOptions.SCHEMA_CHANGE_ENABLED, true);
        if (startupMode != null) {
            config.set(PostgresDataSourceOptions.SCAN_STARTUP_MODE, startupMode);
        }
        return (FlinkSourceProvider)
                new PostgresDataSourceFactory()
                        .createDataSource(
                                new FactoryHelper.DefaultContext(
                                        config, new Configuration(), getClass().getClassLoader()))
                        .getEventSourceProvider();
    }

    private static RunningJob start(FlinkSourceProvider sourceProvider, @Nullable String savepoint)
            throws Exception {
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();
        if (savepoint != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
            // Only the source is restored, every job collects into a new sink.
            configuration.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(500L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 1000L);

        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());
        source.uid("postgres-source");
        TypeSerializer<Event> serializer =
                source.getType().createSerializer(env.getConfig().getSerializerConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<Event> sinkFactory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectStreamSink<Event> sink = new CollectStreamSink<>(source, sinkFactory);
        String sinkUid = "collect-sink-" + UUID.randomUUID();
        sink.name(sinkUid).uid(sinkUid);
        env.addOperator(sink.getTransformation());
        CollectResultIterator<Event> iterator =
                new CollectResultIteratorAdapter<>(
                        new CheckpointedCollectResultBuffer<>(serializer),
                        sinkUid,
                        (CollectSinkOperator<Event>) sinkFactory.getOperator(),
                        accumulatorName,
                        0);
        env.registerCollectIterator(iterator);

        JobClient client = env.executeAsync();
        iterator.setJobClient(client);
        return new RunningJob(client, iterator);
    }

    /** The highest execution attempt number of the job, i.e. the number of failovers. */
    private static int failovers(MiniCluster miniCluster, RunningJob job) throws Exception {
        return StreamSupport.stream(
                        miniCluster
                                .getExecutionGraph(job.client.getJobID())
                                .get()
                                .getAllExecutionVertices()
                                .spliterator(),
                        false)
                .mapToInt(vertex -> vertex.getCurrentExecutionAttempt().getAttemptNumber())
                .max()
                .orElse(0);
    }

    private static List<CreateTableEvent> createTableEvents(List<Event> events) {
        return events.stream()
                .filter(CreateTableEvent.class::isInstance)
                .map(CreateTableEvent.class::cast)
                .collect(Collectors.toList());
    }

    /** No child partition identity in any schema or data change. */
    private static void assertOnlyParent(List<Event> events, TableId parent) {
        assertThat(events)
                .filteredOn(SchemaChangeEvent.class::isInstance)
                .allSatisfy(e -> assertThat(((SchemaChangeEvent) e).tableId()).isEqualTo(parent));
        assertThat(events)
                .filteredOn(DataChangeEvent.class::isInstance)
                .allSatisfy(e -> assertThat(((DataChangeEvent) e).tableId()).isEqualTo(parent));
    }

    private static void awaitSlot(String slot, String condition) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000L;
        String sql =
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '"
                        + slot
                        + "' AND "
                        + condition;
        while (!"1".equals(queryString(sql))) {
            assertThat(System.currentTimeMillis())
                    .as("slot %s satisfies %s within 120s", slot, condition)
                    .isLessThan(deadline);
            Thread.sleep(200L);
        }
    }

    private static String queryString(String sql) throws SQLException {
        try (Connection connection = connect();
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }

    private static void execute(String... statements) throws SQLException {
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword());
    }

    /** A job whose output is collected in the background. */
    private static final class RunningJob implements AutoCloseable {

        private final JobClient client;
        private final CollectResultIterator<Event> iterator;
        private final List<Event> events = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private RunningJob(JobClient client, CollectResultIterator<Event> iterator) {
            this.client = client;
            this.iterator = iterator;
            Thread collector =
                    new Thread(
                            () -> {
                                try {
                                    while (iterator.hasNext()) {
                                        events.add(iterator.next());
                                    }
                                } catch (Throwable t) {
                                    failure.set(t);
                                }
                            });
            collector.setDaemon(true);
            collector.start();
        }

        void awaitParentRows(TableId parent, Integer... ids) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 120_000L;
            while (!parentRowIds(parent).containsAll(Arrays.asList(ids))) {
                if (failure.get() != null) {
                    throw new AssertionError("job failed", failure.get());
                }
                assertThat(System.currentTimeMillis())
                        .as(
                                "rows %s of %s within 120s, received: %s",
                                Arrays.toString(ids), parent, events)
                        .isLessThan(deadline);
                Thread.sleep(200L);
            }
        }

        private Set<Integer> parentRowIds(TableId parent) {
            return events.stream()
                    .filter(DataChangeEvent.class::isInstance)
                    .map(DataChangeEvent.class::cast)
                    .filter(e -> e.tableId().equals(parent) && e.after() != null)
                    .map(e -> e.after().getInt(0))
                    .collect(Collectors.toSet());
        }

        /** Stops the job with a savepoint, retried while the source cannot checkpoint yet. */
        String stopWithSavepoint(String directory) throws Exception {
            long deadline = System.currentTimeMillis() + 60_000L;
            while (true) {
                try {
                    return client.stopWithSavepoint(false, directory, SavepointFormatType.DEFAULT)
                            .get();
                } catch (Exception e) {
                    if (System.currentTimeMillis() > deadline) {
                        throw e;
                    }
                    Thread.sleep(200L);
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                client.cancel().get();
            } catch (Exception alreadyTerminated) {
                // The job was stopped with a savepoint or failed.
            }
            // Wait for the source to release its connections and the slot before the next job.
            while (!client.getJobStatus().get().isGloballyTerminalState()) {
                Thread.sleep(100L);
            }
            iterator.close();
        }
    }
}
