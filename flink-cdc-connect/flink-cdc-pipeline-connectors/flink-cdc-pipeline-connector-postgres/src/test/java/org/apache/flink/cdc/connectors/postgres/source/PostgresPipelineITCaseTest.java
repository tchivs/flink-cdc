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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for Postgres source. */
public class PostgresPipelineITCaseTest extends PostgresTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPipelineITCaseTest.class);
    private static final LocalDate UNIX_EPOCH = LocalDate.of(1970, 1, 1);

    // Common row types for reuse
    private static final RowType PRODUCTS_ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT().notNull(),
                        DataTypes.VARCHAR(255).notNull(),
                        DataTypes.VARCHAR(45),
                        DataTypes.DOUBLE()
                    },
                    new String[] {"id", "name", "description", "weight"});

    private static final RowType ORDERS_ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT().notNull(),
                        DataTypes.INT().notNull(),
                        DataTypes.INT().notNull(),
                        DataTypes.DATE().notNull(),
                        DataTypes.DECIMAL(12, 2).notNull(),
                        DataTypes.STRING(),
                        DataTypes.TIMESTAMP()
                    },
                    new String[] {
                        "id",
                        "order_number",
                        "customer_id",
                        "order_date",
                        "total_amount",
                        "order_status",
                        "processed_at"
                    });

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER, "inventory", "inventory", TEST_USER, TEST_PASSWORD);
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private String slotName;

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws SQLException, InterruptedException {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        inventoryDatabase.removeSlot(slotName);
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        inventoryDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryDatabase.getDatabaseName())
                                .tableList("inventory.products")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(inventoryDatabase.getDatabaseName());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId("inventory", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    @Test
    public void testPartitionTableParentConfiguration() throws Exception {
        inventoryDatabase.createAndInitialize();
        Configuration sourceConfiguration = createBaseConfiguration();
        sourceConfiguration.set(
                PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP, true);
        sourceConfiguration.set(
                PostgresDataSourceOptions.TABLES,
                inventoryDatabase.getDatabaseName() + ".inventory.order_history");
        configurePartitionTableMapping(
                sourceConfiguration, "inventory.order_history", "inventory.order_history_\\d{4}");

        CloseableIterator<Event> events = createEventSource(sourceConfiguration);

        TableId tableId = TableId.tableId("inventory", "order_history");
        CreateTableEvent createTableEvent = getOrdersCreateTableEvent(tableId);
        List<Event> expectedSnapshot = getOrdersSnapshotExpected(tableId);
        Thread.sleep(1000);
        List<Event> actual = fetchResultsExcept(events, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        List<Event> expectedStreamingChanges = executeOrderHistoryInserts(tableId);
        List<Event> streamingChanges =
                fetchResultsExcept(events, expectedStreamingChanges.size(), createTableEvent);
        assertEventsMatch(streamingChanges, expectedStreamingChanges);
    }

    @Test
    public void testPartitionTableOffsetParentConfiguration() throws Exception {
        inventoryDatabase.createAndInitialize();

        Configuration sourceConfiguration = createBaseConfiguration();
        sourceConfiguration.set(PostgresDataSourceOptions.SCAN_STARTUP_MODE, "latest-offset");
        sourceConfiguration.set(
                PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP, false);
        sourceConfiguration.set(
                PostgresDataSourceOptions.TABLES,
                inventoryDatabase.getDatabaseName() + ".inventory.order_history");
        configurePartitionTableMapping(
                sourceConfiguration, "inventory.order_history", "inventory.order_history_\\d{4}");

        CloseableIterator<Event> events = createEventSource(sourceConfiguration);

        TableId tableId = TableId.tableId("inventory", "order_history");
        CreateTableEvent createTableEvent = getOrdersCreateTableEvent(tableId);
        Thread.sleep(1000);
        List<Event> expectedStreamingChanges = executeOrderHistoryInserts(tableId);
        List<Event> streamingChanges =
                fetchResultsExcept(events, expectedStreamingChanges.size(), createTableEvent);
        assertEventsMatch(streamingChanges, expectedStreamingChanges);
    }

    @Test
    public void testInitialStartupModeWithOpts() throws Exception {
        inventoryDatabase.createAndInitialize();

        Configuration sourceConfiguration = createBaseConfiguration();
        sourceConfiguration.set(
                PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP, false);
        sourceConfiguration.set(
                PostgresDataSourceOptions.TABLES,
                inventoryDatabase.getDatabaseName() + ".inventory.products");
        sourceConfiguration.set(PostgresDataSourceOptions.METADATA_LIST, "op_ts");

        CloseableIterator<Event> events = createEventSource(sourceConfiguration);

        TableId tableId = TableId.tableId("inventory", "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("op_ts", "0");

        // generate snapshot data
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(),
                                            dataChangeEvent.after(),
                                            meta);
                                })
                        .collect(Collectors.toList());

        String startTime = String.valueOf(System.currentTimeMillis());
        Thread.sleep(1000);

        List<Event> expectedlog = executeProductsInserts(tableId);
        Thread.sleep(1000);

        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        int snapshotRecordsCount = expectedSnapshot.size();
        int logRecordsCount = expectedlog.size();

        // Ditto, CreateTableEvent might be emitted in multiple partitions.
        List<Event> actual =
                fetchResultsExcept(
                        events, snapshotRecordsCount + logRecordsCount, createTableEvent);

        List<Event> actualSnapshotEvents = actual.subList(0, snapshotRecordsCount);
        List<Event> actuallogEvents = actual.subList(snapshotRecordsCount, actual.size());

        assertThat(actualSnapshotEvents).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        assertThat(actuallogEvents).hasSize(logRecordsCount);

        for (int i = 0; i < logRecordsCount; i++) {
            if (expectedlog.get(i) instanceof SchemaChangeEvent) {
                assertThat(actuallogEvents.get(i)).isEqualTo(expectedlog.get(i));
            } else {
                DataChangeEvent expectedEvent = (DataChangeEvent) expectedlog.get(i);
                DataChangeEvent actualEvent = (DataChangeEvent) actuallogEvents.get(i);
                assertThat(actualEvent.op()).isEqualTo(expectedEvent.op());
                assertThat(actualEvent.before()).isEqualTo(expectedEvent.before());
                assertThat(actualEvent.after()).isEqualTo(expectedEvent.after());
                assertThat(actualEvent.meta().get("op_ts")).isGreaterThanOrEqualTo(startTime);
            }
        }
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (!event.equals(sideEvent)) {
                result.add(event);
                size--;
            } else {
                sideResults.add(sideEvent);
            }
        }
        // Also ensure we've received at least one or many side events.
        assertThat(sideResults).isNotEmpty();
        return result;
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"id", "name", "description", "weight"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    3.14
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    8.1
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    0.8
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenter's hammer"),
                                    0.75
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenter's hammer"),
                                    0.875
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenter's hammer"),
                                    1.0
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    5.3
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    0.1
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    22.2
                                })));
        return snapshotExpected;
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn(
                                "id",
                                DataTypes.INT().notNull(),
                                null,
                                "nextval(\'inventory.products_id_seq\'::regclass)")
                        .physicalColumn(
                                "name",
                                DataTypes.VARCHAR(255).notNull(),
                                null,
                                "'flink'::character varying")
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }

    private CreateTableEvent getOrdersCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn(
                                "id",
                                DataTypes.INT().notNull(),
                                null,
                                "nextval(\'inventory.order_history_id_seq\'::regclass)")
                        .physicalColumn("order_number", DataTypes.INT().notNull())
                        .physicalColumn("customer_id", DataTypes.INT().notNull())
                        .physicalColumn("order_date", DataTypes.DATE().notNull())
                        .physicalColumn("total_amount", DataTypes.DECIMAL(12, 2).notNull())
                        .physicalColumn(
                                "order_status",
                                DataTypes.VARCHAR(20),
                                null,
                                "'PENDING'::character varying")
                        .physicalColumn("processed_at", DataTypes.TIMESTAMP())
                        .primaryKey(java.util.Arrays.asList("id", "order_date"))
                        .build());
    }

    private List<Event> getOrdersSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.DATE().notNull(),
                            DataTypes.DECIMAL(12, 2).notNull(),
                            DataTypes.STRING(),
                            DataTypes.TIMESTAMP()
                        },
                        new String[] {
                            "id",
                            "order_number",
                            "customer_id",
                            "order_date",
                            "total_amount",
                            "order_status",
                            "processed_at"
                        });
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();

        // Expected data from partition tables (matching DDL file data)
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1,
                                    1001,
                                    1001,
                                    getDaysSinceUnixEpoch("2023-06-15"),
                                    DecimalData.fromBigDecimal(new BigDecimal("299.99"), 12, 2),
                                    BinaryStringData.fromString("COMPLETED"),
                                    TimestampData.fromLocalDateTime(
                                            LocalDateTime.of(2023, 6, 15, 10, 30, 0))
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    2,
                                    1002,
                                    1002,
                                    getDaysSinceUnixEpoch("2023-08-20"),
                                    DecimalData.fromBigDecimal(new BigDecimal("145.75"), 12, 2),
                                    BinaryStringData.fromString("SHIPPED"),
                                    TimestampData.fromLocalDateTime(
                                            LocalDateTime.of(2023, 8, 20, 14, 20, 0))
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    3,
                                    1003,
                                    1003,
                                    getDaysSinceUnixEpoch("2024-01-10"),
                                    DecimalData.fromBigDecimal(new BigDecimal("89.50"), 12, 2),
                                    BinaryStringData.fromString("PROCESSING"),
                                    null
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    4,
                                    1004,
                                    1001,
                                    getDaysSinceUnixEpoch("2024-03-18"),
                                    DecimalData.fromBigDecimal(new BigDecimal("199.99"), 12, 2),
                                    BinaryStringData.fromString("COMPLETED"),
                                    TimestampData.fromLocalDateTime(
                                            LocalDateTime.of(2024, 3, 18, 16, 45, 0))
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    5,
                                    1005,
                                    1004,
                                    getDaysSinceUnixEpoch("2024-05-22"),
                                    DecimalData.fromBigDecimal(new BigDecimal("349.99"), 12, 2),
                                    BinaryStringData.fromString("SHIPPED"),
                                    TimestampData.fromLocalDateTime(
                                            LocalDateTime.of(2024, 5, 22, 11, 15, 0))
                                })));
        return snapshotExpected;
    }

    public static long getDaysSinceUnixEpoch(String dateStr) {
        try {
            LocalDate date = LocalDate.parse(dateStr);
            return ChronoUnit.DAYS.between(UNIX_EPOCH, date);
        } catch (DateTimeParseException e) {
            throw new DateTimeParseException(
                    "无法解析日期: " + dateStr + "，请使用yyyy-MM-dd格式", dateStr, e.getErrorIndex(), e);
        }
    }

    /** Creates a common base configuration for PostgreSQL CDC source. */
    private Configuration createBaseConfiguration() {
        Configuration config = new Configuration();
        config.set(PostgresDataSourceOptions.HOSTNAME, POSTGRES_CONTAINER.getHost());
        config.set(
                PostgresDataSourceOptions.PG_PORT,
                POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT));
        config.set(PostgresDataSourceOptions.USERNAME, TEST_USER);
        config.set(PostgresDataSourceOptions.PASSWORD, TEST_PASSWORD);
        config.set(PostgresDataSourceOptions.SLOT_NAME, slotName);
        config.set(PostgresDataSourceOptions.DECODING_PLUGIN_NAME, "pgoutput");
        config.set(PostgresDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        return config;
    }

    /** Creates a CloseableIterator of events from the given configuration. */
    private CloseableIterator<Event> createEventSource(Configuration sourceConfiguration)
            throws Exception {
        Factory.Context context =
                new FactoryHelper.DefaultContext(
                        sourceConfiguration, new Configuration(), this.getClass().getClassLoader());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();

        return env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        PostgresDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo())
                .executeAndCollect();
    }

    /** Asserts that streaming changes match expected events using manual comparison. */
    private void assertEventsMatch(List<Event> actualEvents, List<Event> expectedEvents) {
        assertThat(actualEvents).hasSize(expectedEvents.size());
        for (Event expectedEvent : expectedEvents) {
            DataChangeEvent expected = (DataChangeEvent) expectedEvent;
            boolean found = false;
            for (Event actualEvent : actualEvents) {
                DataChangeEvent actual = (DataChangeEvent) actualEvent;
                if (actual.tableId().equals(expected.tableId())
                        && actual.op().equals(expected.op())
                        && actual.after().equals(expected.after())
                        && actual.before() == expected.before()) {
                    found = true;
                    break;
                }
            }
            assertThat(found).as("Expected event not found: " + expected).isTrue();
        }
    }

    /** Configures partition table mapping for the given configuration. */
    private void configurePartitionTableMapping(
            Configuration config, String parentTable, String pattern) {
        config.set(
                ConfigOptions.key(
                                String.format(
                                        "partition-tables.%s.%s",
                                        inventoryDatabase.getDatabaseName(), parentTable))
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Partition table mapping configuration"),
                inventoryDatabase.getDatabaseName() + "." + pattern);
    }

    /** Executes partition table inserts and returns expected streaming change events. */
    private List<Event> executeOrderHistoryInserts(TableId tableId) throws Exception {
        List<Event> expectedStreamingChanges = new ArrayList<>();
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(ORDERS_ROW_TYPE);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, this.inventoryDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {

            // Insert into different partitions
            statement.execute(
                    "INSERT INTO inventory.order_history (order_number, customer_id, order_date, total_amount, order_status, processed_at) "
                            + "VALUES (1006, 1005, '2023-01-25', 149.99, 'PENDING', '2023-01-25 10:30:00')"); // id=6
            expectedStreamingChanges.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        6,
                                        1006,
                                        1005,
                                        getDaysSinceUnixEpoch("2023-01-25"),
                                        DecimalData.fromBigDecimal(new BigDecimal("149.99"), 12, 2),
                                        BinaryStringData.fromString("PENDING"),
                                        TimestampData.fromLocalDateTime(
                                                LocalDateTime.of(2023, 1, 25, 10, 30, 0))
                                    })));

            statement.execute(
                    "INSERT INTO inventory.order_history (order_number, customer_id, order_date, total_amount, order_status, processed_at) "
                            + "VALUES (1007, 1006, '2024-04-15', 299.97, 'COMPLETED', '2024-04-15 14:20:00')"); // id=7
            expectedStreamingChanges.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        7,
                                        1007,
                                        1006,
                                        getDaysSinceUnixEpoch("2024-04-15"),
                                        DecimalData.fromBigDecimal(new BigDecimal("299.97"), 12, 2),
                                        BinaryStringData.fromString("COMPLETED"),
                                        TimestampData.fromLocalDateTime(
                                                LocalDateTime.of(2024, 4, 15, 14, 20, 0))
                                    })));
        }
        Thread.sleep(1000);
        return expectedStreamingChanges;
    }

    /** Executes products table inserts and returns expected streaming change events. */
    private List<Event> executeProductsInserts(TableId tableId) throws Exception {
        List<Event> expectedLog = new ArrayList<>();
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(PRODUCTS_ROW_TYPE);

        try (Connection connection =
                        getJdbcConnection(POSTGRES_CONTAINER, inventoryDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {

            statement.execute(
                    String.format(
                            "INSERT INTO %s VALUES (default,'scooter','c-2',5.5);",
                            "inventory.products")); // 110
            expectedLog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5
                                    })));

            statement.execute(
                    String.format(
                            "INSERT INTO %s VALUES (default,'football','c-11',6.6);",
                            "inventory.products")); // 111
            expectedLog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6
                                    })));

            statement.execute(
                    String.format(
                            "UPDATE %s SET description='c-12' WHERE id=110;",
                            "inventory.products"));
            expectedLog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-2"),
                                        5.5
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-12"),
                                        5.5
                                    })));

            statement.execute(
                    String.format("DELETE FROM %s WHERE id = 111;", "inventory.products"));
            expectedLog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6
                                    })));
        }
        return expectedLog;
    }
}
