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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.MockPostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.io.InputStatus.END_OF_INPUT;
import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link PostgresSourceReader}. */
public class PostgresSourceReaderTest extends PostgresTestBase {
    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";
    private String slotName;
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @BeforeEach
    public void before() throws SQLException {
        customDatabase.createAndInitialize();

        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        PostgresSourceReader.clearSnapshotStreamSplitHook();
        customDatabase.removeSlot(slotName);
    }

    @Test
    void testNotifyCheckpointWindowSizeOne() throws Exception {
        final PostgresSourceReader reader = createReader(1);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(11L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(12L);
        assertThat(completedCheckpointIds).containsExactly(11L);
        reader.notifyCheckpointComplete(13L);
        assertThat(completedCheckpointIds).containsExactly(11L, 12L);
    }

    @Test
    void testNotifyCheckpointWindowSizeDefault() throws Exception {
        final PostgresSourceReader reader = createReader(3);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(103L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(102L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(101L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(104L);
        assertThat(completedCheckpointIds).containsExactly(101L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultipleSnapshotSplit(boolean skipBackFill) throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("Id", DataTypes.BIGINT()),
                        DataTypes.FIELD("Name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));

        TableId tableId = new TableId(null, SCHEMA_NAME, "Customers");
        RowType splitType =
                RowType.of(
                        new LogicalType[] {DataTypes.INT().getLogicalType()}, new String[] {"Id"});
        List<SnapshotSplit> snapshotSplits =
                Arrays.asList(
                        new SnapshotSplit(
                                tableId,
                                0,
                                splitType,
                                null,
                                new Integer[] {200},
                                null,
                                Collections.emptyMap()),
                        new SnapshotSplit(
                                tableId,
                                1,
                                splitType,
                                new Integer[] {200},
                                new Integer[] {1500},
                                null,
                                Collections.emptyMap()),
                        new SnapshotSplit(
                                tableId,
                                2,
                                splitType,
                                new Integer[] {1500},
                                null,
                                null,
                                Collections.emptyMap()));

        // Step 1: start source reader and assign snapshot splits
        PostgresSourceReader reader = createReader(-1, skipBackFill);
        reader.start();
        reader.addSplits(snapshotSplits);

        String[] expectedRecords =
                new String[] {
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        // Step 2: wait the snapshot splits finished reading
        List<String> actualRecords = consumeSnapshotRecords(reader, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expectedRecords), actualRecords);
    }

    @Test
    void testSchemaChangeUpdatesSnapshotState() throws Exception {
        PostgresSourceReader reader = createStreamReader();
        reader.start();

        // Discover table schemas and create a stream split
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        PostgresSourceConfig sourceConfig = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(sourceConfig);

        TableId tableId = new TableId(null, SCHEMA_NAME, "Customers");
        // Verify original schema has 4 columns (Id, Name, address, phone_number)
        assertThat(tableSchemas.get(tableId).getTable().columns()).hasSize(4);
        assertThat(tableSchemas.get(tableId).getTable().columnWithName("email")).isNull();

        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        StreamSplit streamSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        offsetFactory.createInitialOffset(),
                        offsetFactory.createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0);
        reader.addSplits(Collections.singletonList(streamSplit));

        // Wait for the reader to start consuming
        Thread.sleep(1000L);

        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, customDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            // Insert a record BEFORE the DDL change
            stmt.execute(
                    "INSERT INTO customer.\"Customers\" VALUES (3001, 'before_ddl', 'Beijing', '111')");

            // Perform a DDL change
            stmt.execute(
                    "ALTER TABLE customer.\"Customers\" ADD COLUMN email VARCHAR(255) DEFAULT 'test@test.com'");

            // Insert a record AFTER the DDL change
            stmt.execute(
                    "INSERT INTO customer.\"Customers\" VALUES (3002, 'after_ddl', 'Shanghai', '222', 'after@test.com')");
        }

        // Wait for the schema change event to be processed
        Thread.sleep(1000L);

        // Poll records so the emitter processes all events
        final SimpleReaderOutput output = new SimpleReaderOutput();
        for (int i = 0; i < 10; i++) {
            reader.pollNext(output);
        }

        // Verify the emitted records contain data before and after DDL in correct order
        List<SourceRecord> results = output.getResults();
        int beforeDdlPos = -1;
        int afterDdlPos = -1;
        for (int i = 0; i < results.size(); i++) {
            SourceRecord record = results.get(i);
            if (record.value() != null) {
                String value = record.value().toString();
                if (value.contains("before_ddl")) {
                    beforeDdlPos = i;
                } else if (value.contains("after_ddl")) {
                    afterDdlPos = i;
                }
            }
        }
        assertThat(beforeDdlPos)
                .as("Should capture the INSERT before DDL")
                .isGreaterThanOrEqualTo(0);
        assertThat(afterDdlPos).as("Should capture the INSERT after DDL").isGreaterThanOrEqualTo(0);
        assertThat(beforeDdlPos)
                .as("INSERT before DDL should appear before INSERT after DDL")
                .isLessThan(afterDdlPos);

        // Verify that snapshotState returns splits with updated table schema
        List<SourceSplitBase> splits = reader.snapshotState(1L);
        assertThat(splits).isNotEmpty();

        boolean foundUpdatedSchema = false;
        for (SourceSplitBase split : splits) {
            if (split.isStreamSplit()) {
                Map<TableId, TableChanges.TableChange> schemas =
                        split.asStreamSplit().getTableSchemas();
                if (schemas.containsKey(tableId)
                        && schemas.get(tableId).getTable().columnWithName("email") != null) {
                    foundUpdatedSchema = true;
                    break;
                }
            }
        }
        assertThat(foundUpdatedSchema)
                .as("The snapshotState should contain the updated table schema with 'email' column")
                .isTrue();
    }

    @Test
    void testRestoredPg10RoutingStateSeedsReaderAndSnapshotsBack() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable, childUs, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk, childUs)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig restoredReaderConfig = extractReaderSourceConfig(reader);
        PostgresDialect restoredDialect = extractReaderDialect(reader);

        assertThat(restoredReaderConfig.isPg10PartitionMappingInitialized()).isTrue();
        assertThat(restoredReaderConfig.getChildToParentMappingOrEmpty())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(restoredReaderConfig.getParentToChildrenMappingOrEmpty().get(parentTable))
                .containsExactly(childUk, childUs);
        assertThat(restoredDialect.createFetchTask(restoredSplit))
                .isInstanceOf(
                        org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamFetchTask
                                .class);

        List<SourceSplitBase> snapshotSplits = reader.snapshotState(1L);
        assertThat(snapshotSplits).hasSize(1);
        StreamSplit snapshottedSplit = snapshotSplits.get(0).asStreamSplit();
        assertThat(snapshottedSplit.getPg10ChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(snapshottedSplit.getPg10ParentToChildrenMapping().get(parentTable))
                .containsExactly(childUk, childUs);
    }

    @Test
    void testRestoredPg10RoutingStateAppliedBeforeSchemaRediscovery() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());
        configFactory.scanNewlyAddedTableEnabled(true);

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");
        Map<TableId, TableChanges.TableChange> restoredTableSchemas =
                createTableSchemas(parentTable, childUk, childUs);

        RecordingMockPostgresDialect dialect =
                new RecordingMockPostgresDialect(
                        dialectConfig, restoredTableSchemas, childUk, childUs);

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        restoredTableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable, childUs, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk, childUs)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);

        reader.addSplits(Collections.singletonList(restoredSplit));

        assertThat(dialect.getSchemaRediscoveryCalls()).isEqualTo(1);
        assertThat(dialect.hasRestoredPg10RoutingStateAtSchemaRediscovery()).isTrue();
        assertThat(dialect.getObservedChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
    }

    @Test
    void testRestoredPg10RoutingStateWinsOverStaleReaderConfig() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable, childUs, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk, childUs)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        PostgresSourceConfig readerConfig = extractReaderSourceConfig(reader);
        readerConfig.setChildToParentMapping(new LinkedHashMap<>(Map.of(childUk, parentTable)));
        readerConfig.setParentToChildrenMapping(
                new LinkedHashMap<>(Map.of(parentTable, new ArrayList<>(List.of(childUk)))));
        readerConfig.setPg10PartitionMappingInitialized(true);

        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig restoredReaderConfig = extractReaderSourceConfig(reader);
        assertThat(restoredReaderConfig.getChildToParentMappingOrEmpty())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(restoredReaderConfig.getParentToChildrenMappingOrEmpty().get(parentTable))
                .containsExactly(childUk, childUs);

        List<SourceSplitBase> snapshotSplits = reader.snapshotState(1L);
        assertThat(snapshotSplits).hasSize(1);
        StreamSplit snapshottedSplit = snapshotSplits.get(0).asStreamSplit();
        assertThat(snapshottedSplit.getPg10ChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(snapshottedSplit.getPg10ParentToChildrenMapping().get(parentTable))
                .containsExactly(childUk, childUs);
    }

    @Test
    void testSnapshotStatePersistsAcceptedRuntimePg10RoutingFromSharedConfig() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig readerConfig = extractReaderSourceConfig(reader);
        readerConfig.setChildToParentMapping(
                new LinkedHashMap<>(Map.of(childUk, parentTable, childUs, parentTable)));
        readerConfig.setParentToChildrenMapping(
                new LinkedHashMap<>(
                        Map.of(parentTable, new ArrayList<>(List.of(childUk, childUs)))));
        readerConfig.setPg10PartitionMappingInitialized(true);

        List<SourceSplitBase> snapshotSplits = reader.snapshotState(1L);
        assertThat(snapshotSplits).hasSize(1);
        StreamSplit snapshottedSplit = snapshotSplits.get(0).asStreamSplit();
        assertThat(snapshottedSplit.getPg10ChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(snapshottedSplit.getPg10ParentToChildrenMapping().get(parentTable))
                .containsExactly(childUk, childUs);
    }

    @Test
    void testSnapshotStateHookObservesMaterializedAcceptedRuntimePg10Routing() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk)))));

        AtomicReference<StreamSplit> observedSnapshotSplit = new AtomicReference<>();
        PostgresSourceReader.setSnapshotStreamSplitHook(observedSnapshotSplit::set);

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig readerConfig = extractReaderSourceConfig(reader);
        readerConfig.setChildToParentMapping(
                new LinkedHashMap<>(Map.of(childUk, parentTable, childUs, parentTable)));
        readerConfig.setParentToChildrenMapping(
                new LinkedHashMap<>(
                        Map.of(parentTable, new ArrayList<>(List.of(childUk, childUs)))));
        readerConfig.setPg10PartitionMappingInitialized(true);

        List<SourceSplitBase> snapshotSplits = reader.snapshotState(1L);
        StreamSplit snapshottedSplit = snapshotSplits.get(0).asStreamSplit();

        assertThat(observedSnapshotSplit.get()).isNotNull();
        assertThat(observedSnapshotSplit.get()).isEqualTo(snapshottedSplit);
        assertThat(observedSnapshotSplit.get().getPg10ChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(observedSnapshotSplit.get().getPg10ParentToChildrenMapping().get(parentTable))
                .containsExactly(childUk, childUs);
    }

    @Test
    void testSnapshotStatePersistsInitializedButEmptyPg10RoutingState() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig readerConfig = extractReaderSourceConfig(reader);
        readerConfig.setChildToParentMapping(new LinkedHashMap<>());
        readerConfig.setParentToChildrenMapping(new LinkedHashMap<>());
        readerConfig.setPg10PartitionMappingInitialized(true);

        List<SourceSplitBase> snapshotSplits = reader.snapshotState(1L);
        StreamSplit snapshottedSplit = snapshotSplits.get(0).asStreamSplit();

        assertThat(snapshottedSplit.getPg10ChildToParentMapping()).isEmpty();
        assertThat(snapshottedSplit.getPg10ParentToChildrenMapping()).isEmpty();
        assertThat(snapshottedSplit.isPg10RoutingStateInitialized()).isTrue();
    }

    @Test
    void testSuspendLifecycleDoesNotReapplyStalePg10RoutingState() throws Exception {
        PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new MockPostgresDialect(dialectConfig);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(dialectConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, "Customers");
        TableId childUk = new TableId(null, SCHEMA_NAME, "Customers_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "Customers_us");

        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0,
                        false,
                        false,
                        new LinkedHashMap<>(Map.of(childUk, parentTable)),
                        new LinkedHashMap<>(
                                Map.of(parentTable, new ArrayList<>(List.of(childUk)))));

        PostgresSourceReader reader = createReaderWithFactory(configFactory, dialect);
        reader.addSplits(Collections.singletonList(restoredSplit));

        PostgresSourceConfig readerConfig = extractReaderSourceConfig(reader);
        readerConfig.setChildToParentMapping(new LinkedHashMap<>(Map.of(childUs, parentTable)));
        readerConfig.setParentToChildrenMapping(
                new LinkedHashMap<>(Map.of(parentTable, new ArrayList<>(List.of(childUs)))));
        readerConfig.setPg10PartitionMappingInitialized(true);

        StreamSplitState staleStreamSplitState = new StreamSplitState(restoredSplit);
        Method onSplitFinished =
                reader.getClass()
                        .getSuperclass()
                        .getSuperclass()
                        .getDeclaredMethod("onSplitFinished", Map.class);
        onSplitFinished.setAccessible(true);
        extractReaderContext(reader).suspendStreamSplitReader();
        Map<String, SourceSplitState> finishedSplitIds =
                new LinkedHashMap<>(Map.of(StreamSplit.STREAM_SPLIT_ID, staleStreamSplitState));
        onSplitFinished.invoke(reader, finishedSplitIds);

        StreamSplit suspendedStreamSplit = extractSuspendedStreamSplit(reader);
        assertThat(suspendedStreamSplit).isNotNull();
        assertThat(suspendedStreamSplit.getPg10ChildToParentMapping())
                .containsOnly(entry(childUs, parentTable));
        assertThat(suspendedStreamSplit.getPg10ParentToChildrenMapping().get(parentTable))
                .containsExactly(childUs);

        reader.handleSourceEvents(new LatestFinishedSplitsNumberEvent(0));
        PostgresSourceConfig restoredReaderConfig = extractReaderSourceConfig(reader);
        assertThat(restoredReaderConfig.getChildToParentMappingOrEmpty())
                .containsOnly(entry(childUs, parentTable));
    }

    private List<String> consumeSnapshotRecords(
            PostgresSourceReader sourceReader, DataType recordType) throws Exception {
        // Poll all the  records of the multiple assigned snapshot split.
        sourceReader.notifyNoMoreSplits();
        final SimpleReaderOutput output = new SimpleReaderOutput();
        InputStatus status = MORE_AVAILABLE;
        while (END_OF_INPUT != status) {
            status = sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    private PostgresSourceReader createReader(final int lsnCommitCheckpointsDelay)
            throws Exception {
        return createReader(lsnCommitCheckpointsDelay, false);
    }

    private PostgresSourceReader createReader(
            final int lsnCommitCheckpointsDelay, boolean skipBackFill) throws Exception {
        final PostgresSourceConfigFactory configFactory = createConfigFactory();
        MockPostgresDialect dialect = new MockPostgresDialect(configFactory.create(0));
        configFactory.setLsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay);
        configFactory.skipSnapshotBackfill(skipBackFill);
        return createReaderWithFactory(configFactory, dialect);
    }

    private PostgresSourceReader createReaderWithFactory(
            PostgresSourceConfigFactory configFactory, PostgresDialect dialect) throws Exception {
        final PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        final PostgresSourceBuilder.PostgresIncrementalSource<?> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory, new ForwardDeserializeSchema(), offsetFactory, dialect);
        return source.createReader(new TestingReaderContext());
    }

    private PostgresSourceReader createStreamReader() throws Exception {
        final PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        final PostgresSourceConfigFactory configFactory = createConfigFactory();
        configFactory.startupOptions(StartupOptions.latest());
        configFactory.setLsnCommitCheckpointsDelay(1);
        PostgresDialect dialect = new PostgresDialect(configFactory.create(0));
        final PostgresSourceBuilder.PostgresIncrementalSource<?> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory, new ForwardDeserializeSchema(), offsetFactory, dialect);
        return source.createReader(new TestingReaderContext());
    }

    private PostgresSourceConfig extractReaderSourceConfig(PostgresSourceReader reader)
            throws Exception {
        Field sourceConfigField =
                reader.getClass().getSuperclass().getSuperclass().getDeclaredField("sourceConfig");
        sourceConfigField.setAccessible(true);
        return (PostgresSourceConfig) sourceConfigField.get(reader);
    }

    private PostgresDialect extractReaderDialect(PostgresSourceReader reader) throws Exception {
        Field dialectField =
                reader.getClass().getSuperclass().getSuperclass().getDeclaredField("dialect");
        dialectField.setAccessible(true);
        return (PostgresDialect) dialectField.get(reader);
    }

    private org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext
            extractReaderContext(PostgresSourceReader reader) throws Exception {
        Field readerContextField =
                reader.getClass()
                        .getSuperclass()
                        .getSuperclass()
                        .getDeclaredField("incrementalSourceReaderContext");
        readerContextField.setAccessible(true);
        return (org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext)
                readerContextField.get(reader);
    }

    private StreamSplit extractSuspendedStreamSplit(PostgresSourceReader reader) throws Exception {
        Field suspendedStreamSplitField =
                reader.getClass()
                        .getSuperclass()
                        .getSuperclass()
                        .getDeclaredField("suspendedStreamSplit");
        suspendedStreamSplitField.setAccessible(true);
        return (StreamSplit) suspendedStreamSplitField.get(reader);
    }

    private PostgresSourceConfigFactory createConfigFactory() {
        final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(customDatabase.getHost());
        configFactory.port(customDatabase.getDatabasePort());
        configFactory.database(customDatabase.getDatabaseName());
        configFactory.tableList(SCHEMA_NAME + ".Customers");
        configFactory.username(customDatabase.getUsername());
        configFactory.password(customDatabase.getPassword());
        configFactory.decodingPluginName("pgoutput");
        configFactory.slotName(slotName);
        return configFactory;
    }

    private Map<TableId, TableChanges.TableChange> createTableSchemas(TableId... tableIds) {
        Map<TableId, TableChanges.TableChange> tableSchemas = new LinkedHashMap<>();
        Tables tables = new Tables();
        for (TableId tableId : tableIds) {
            Table table = tables.editOrCreateTable(tableId).create();
            tableSchemas.put(
                    tableId,
                    new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table));
        }
        return tableSchemas;
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    private static class SimpleReaderOutput implements ReaderOutput<SourceRecord> {

        private final List<SourceRecord> results = new ArrayList<>();

        @Override
        public void collect(SourceRecord record) {
            results.add(record);
        }

        public List<SourceRecord> getResults() {
            return results;
        }

        @Override
        public void collect(SourceRecord record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<SourceRecord> createOutputForSplit(java.lang.String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(java.lang.String splitId) {}
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }

    private static class RecordingMockPostgresDialect extends MockPostgresDialect {

        private final Map<TableId, TableChanges.TableChange> rediscoveredTableSchemas;
        private final TableId expectedChildUk;
        private final TableId expectedChildUs;
        private int schemaRediscoveryCalls;
        private boolean restoredPg10RoutingStateAtSchemaRediscovery;
        private Map<TableId, TableId> observedChildToParentMapping = Collections.emptyMap();

        private RecordingMockPostgresDialect(
                PostgresSourceConfig sourceConfig,
                Map<TableId, TableChanges.TableChange> rediscoveredTableSchemas,
                TableId expectedChildUk,
                TableId expectedChildUs) {
            super(sourceConfig);
            this.rediscoveredTableSchemas = rediscoveredTableSchemas;
            this.expectedChildUk = expectedChildUk;
            this.expectedChildUs = expectedChildUs;
        }

        @Override
        public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
                JdbcSourceConfig sourceConfig) {
            schemaRediscoveryCalls++;
            PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
            observedChildToParentMapping =
                    new LinkedHashMap<>(postgresSourceConfig.getChildToParentMappingOrEmpty());
            restoredPg10RoutingStateAtSchemaRediscovery =
                    observedChildToParentMapping.containsKey(expectedChildUk)
                            && observedChildToParentMapping.containsKey(expectedChildUs);
            return new LinkedHashMap<>(rediscoveredTableSchemas);
        }

        private int getSchemaRediscoveryCalls() {
            return schemaRediscoveryCalls;
        }

        private boolean hasRestoredPg10RoutingStateAtSchemaRediscovery() {
            return restoredPg10RoutingStateAtSchemaRediscovery;
        }

        private Map<TableId, TableId> getObservedChildToParentMapping() {
            return observedChildToParentMapping;
        }
    }
}
