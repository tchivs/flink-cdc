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
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresEventDeserializer;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Unit tests for {@link PostgresPipelineRecordEmitter}. */
class PostgresPipelineRecordEmitterTest extends PostgresTestBase {

    private final UniqueDatabase inventoryPartitionedDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres_pipeline_emitter",
                    "inventory_partitioned",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    void applySplitWithParentAndChildrenSchemasOnlyKeepsParentCreateTableEvent() throws Exception {
        inventoryPartitionedDatabase.createAndInitialize();

        TableId parent = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfig);
        postgresDialect.setCurrentPartitionState(
                PartitionCaptureState.of(
                        Collections.singletonMap(parent, java.util.Arrays.asList(childUk, childUs)),
                        true,
                        "dbz_publication"));
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        PostgresPipelineRecordEmitter<org.apache.flink.cdc.common.event.Event> emitter =
                createEmitter(deserializer, sourceConfig, postgresDialect);
        deserializer.getCreateTableEventCache().clear();

        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        try (JdbcConnection jdbc = postgresDialect.openJdbcConnection(sourceConfig)) {
            tableSchemas.put(parent, postgresDialect.queryTableSchema(jdbc, parent));
            tableSchemas.put(childUk, postgresDialect.queryTableSchema(jdbc, childUk));
            tableSchemas.put(childUs, postgresDialect.queryTableSchema(jdbc, childUs));
        }
        emitter.applySplit(snapshotSplit(childUs, tableSchemas));

        Map<TableId, CreateTableEvent> createTableEventCache =
                deserializer.getCreateTableEventCache();
        Assertions.assertThat(createTableEventCache).containsOnlyKeys(parent);
        Assertions.assertThat(createTableEventCache).doesNotContainKeys(childUk, childUs);
        Assertions.assertThat(createTableEventCache.get(parent).tableId())
                .isEqualTo(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                "inventory_partitioned", "products"));
    }

    @Test
    void applySplitRoutesPartitionChildCreateTableEventToParent() throws Exception {
        inventoryPartitionedDatabase.createAndInitialize();

        TableId parent = new TableId(null, "inventory_partitioned", "products");
        TableId child = new TableId(null, "inventory_partitioned", "products_us");
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfig);
        postgresDialect.setCurrentPartitionState(
                PartitionCaptureState.of(
                        Collections.singletonMap(parent, Collections.singletonList(child)),
                        true,
                        "dbz_publication"));
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        PostgresPipelineRecordEmitter<org.apache.flink.cdc.common.event.Event> emitter =
                createEmitter(deserializer, sourceConfig, postgresDialect);
        deserializer.getCreateTableEventCache().clear();

        TableChanges.TableChange childTableSchema;
        try (JdbcConnection jdbc = postgresDialect.openJdbcConnection(sourceConfig)) {
            childTableSchema = postgresDialect.queryTableSchema(jdbc, child);
        }
        emitter.applySplit(snapshotSplit(child, childTableSchema));

        Map<TableId, CreateTableEvent> createTableEventCache =
                deserializer.getCreateTableEventCache();
        Assertions.assertThat(createTableEventCache).containsOnlyKeys(parent);
        Assertions.assertThat(createTableEventCache).doesNotContainKey(child);
        Assertions.assertThat(createTableEventCache.get(parent).tableId())
                .isEqualTo(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                "inventory_partitioned", "products"));
    }

    @Test
    void streamSchemaChangeFromPartitionChildRecordsAndEmitsParentOnly() throws Exception {
        inventoryPartitionedDatabase.createAndInitialize();

        TableId parent = new TableId(null, "inventory_partitioned", "products");
        TableId child = new TableId(null, "inventory_partitioned", "products_us");
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfig);
        postgresDialect.setCurrentPartitionState(
                PartitionCaptureState.of(
                        Collections.singletonMap(parent, Collections.singletonList(child)),
                        true,
                        "dbz_publication"));
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        PostgresPipelineRecordEmitter<Event> emitter =
                createEmitter(deserializer, sourceConfig, postgresDialect);
        deserializer.getCreateTableEventCache().clear();

        TableChanges.TableChange childTableSchema;
        try (JdbcConnection jdbc = postgresDialect.openJdbcConnection(sourceConfig)) {
            childTableSchema = postgresDialect.queryTableSchema(jdbc, child);
        }

        StreamSplitState streamSplitState = streamSplitState();
        CollectingSourceOutput output = new CollectingSourceOutput();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(
                        new PostgresSchemaRecord(childTableSchema.getTable())),
                output,
                streamSplitState);

        Assertions.assertThat(output.events)
                .allSatisfy(
                        event -> {
                            Assertions.assertThat(event)
                                    .as("Only parent schema events should be emitted")
                                    .isInstanceOf(CreateTableEvent.class);
                            Assertions.assertThat(((CreateTableEvent) event).tableId())
                                    .isEqualTo(
                                            org.apache.flink.cdc.common.event.TableId.tableId(
                                                    "inventory_partitioned", "products"));
                        });
        Assertions.assertThat(output.events).hasSize(1);
        Assertions.assertThat(streamSplitState.getTableSchemas()).containsOnlyKeys(parent);
        Assertions.assertThat(streamSplitState.getTableSchemas()).doesNotContainKey(child);
    }

    private static PostgresPipelineRecordEmitter<org.apache.flink.cdc.common.event.Event>
            createEmitter(
                    PostgresEventDeserializer deserializer,
                    PostgresSourceConfig sourceConfig,
                    PostgresDialect postgresDialect) {
        return new PostgresPipelineRecordEmitter<>(
                deserializer,
                new SourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(
                                new org.apache.flink.metrics.groups.UnregisteredMetricsGroup())),
                sourceConfig,
                new PostgresOffsetFactory(),
                postgresDialect);
    }

    private PostgresSourceConfig sourceConfig() {
        PostgresSourceConfigFactory factory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGRES_CONTAINER.getHost())
                                .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(inventoryPartitionedDatabase.getDatabaseName())
                                .tableList("inventory_partitioned.products")
                                .startupOptions(StartupOptions.initial())
                                .includeSchemaChanges(true)
                                .serverTimeZone("UTC");
        factory.database(inventoryPartitionedDatabase.getDatabaseName());
        factory.decodingPluginName("pgoutput");
        factory.setIncludePartitionedTables(true);
        return factory.create(0);
    }

    private static StreamSplitState streamSplitState() {
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        return new StreamSplitState(
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        offsetFactory.createInitialOffset(),
                        offsetFactory.createNoStoppingOffset(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        0));
    }

    private static SnapshotSplit snapshotSplit(
            TableId tableId, TableChanges.TableChange tableSchema) {
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(tableId, tableSchema);
        return new SnapshotSplit(
                tableId,
                0,
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.IntType(false)),
                null,
                null,
                null,
                tableSchemas);
    }

    private static SnapshotSplit snapshotSplit(
            TableId tableId, Map<TableId, TableChanges.TableChange> tableSchemas) {
        return new SnapshotSplit(
                tableId,
                0,
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.IntType(false)),
                null,
                null,
                null,
                tableSchemas);
    }

    private static class CollectingSourceOutput implements SourceOutput<Event> {
        private final List<Event> events = new ArrayList<>();

        @Override
        public void collect(Event event) {
            events.add(event);
        }

        @Override
        public void collect(Event event, long timestamp) {
            collect(event);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
