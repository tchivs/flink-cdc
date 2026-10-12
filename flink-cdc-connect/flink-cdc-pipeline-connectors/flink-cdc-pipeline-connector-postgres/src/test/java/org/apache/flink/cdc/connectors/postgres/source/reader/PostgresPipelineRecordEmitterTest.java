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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresEventDeserializer;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PostgresPipelineRecordEmitter}. */
class PostgresPipelineRecordEmitterTest {

    private static final TableId ROOT = new TableId(null, "public", "jjdb");
    private static final TableId SAMPLE = new TableId(null, "public", "jjdb_202609");
    private static final TableId SWAPPED = new TableId(null, "public", "jjdb_202610");

    @Test
    void routedRootTakesSampleChildWithoutLookingUpTheRoot() {
        Map<TableId, Table> visible = tables(SAMPLE, SWAPPED);

        assertThat(
                        PostgresPipelineRecordEmitter.partitionOf(
                                        ROOT,
                                        Optional.of("public.jjdb_202609"),
                                        SWAPPED,
                                        visible::get)
                                .id())
                .isEqualTo(SAMPLE);
    }

    @Test
    void routedRootFallsBackToEventPartitionWhenSampleChildIsGone() {
        Map<TableId, Table> visible = tables(SWAPPED);

        assertThat(
                        PostgresPipelineRecordEmitter.partitionOf(
                                        ROOT,
                                        Optional.of("public.jjdb_202609"),
                                        SWAPPED,
                                        visible::get)
                                .id())
                .isEqualTo(SWAPPED);
    }

    @Test
    void routedRootFailsWithTableNamesWhenNoPartitionIsVisible() {
        Map<TableId, Table> visible = tables(ROOT);

        assertThatThrownBy(
                        () ->
                                PostgresPipelineRecordEmitter.partitionOf(
                                        ROOT,
                                        Optional.of("public.jjdb_202609"),
                                        SWAPPED,
                                        visible::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("public.jjdb")
                .hasMessageContaining("public.jjdb_202609")
                .hasMessageContaining("public.jjdb_202610");
    }

    @Test
    void columnAddedByEveryPartitionIsEmittedForTheRootOnce() {
        org.apache.flink.cdc.common.event.TableId parent =
                org.apache.flink.cdc.common.event.TableId.tableId("db", "public", "jjdb");
        Schema initial =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        SchemaChangeEvent addColumn =
                new AddColumnEvent(
                        parent,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("name", DataTypes.STRING()))));
        Map<TableId, Schema> parentSchemas = new HashMap<>();

        List<SchemaChangeEvent> fromFirstPartition =
                PostgresPipelineRecordEmitter.evolveParentSchema(
                        parentSchemas, ROOT, initial, Collections.singletonList(addColumn));
        List<SchemaChangeEvent> fromSecondPartition =
                PostgresPipelineRecordEmitter.evolveParentSchema(
                        parentSchemas, ROOT, initial, Collections.singletonList(addColumn));

        assertThat(fromFirstPartition).containsExactly(addColumn);
        assertThat(fromSecondPartition).isEmpty();
        assertThat(parentSchemas.get(ROOT).getColumnNames()).containsExactly("id", "name");
    }

    private static Map<TableId, Table> tables(TableId... ids) {
        Map<TableId, Table> tables = new HashMap<>();
        for (TableId id : ids) {
            tables.put(id, Table.editor().tableId(id).create());
        }
        return tables;
    }

    private static final TableId DBZ_TABLE_ID = new TableId(null, "public", "users");

    @Test
    void testInitializationDoesNotLoadTableSchemas() {
        PostgresSourceConfig sourceConfig = createSourceConfig(StartupOptions.initial());

        assertThatCode(() -> createEmitter(sourceConfig)).doesNotThrowAnyException();
    }

    @Test
    void testSnapshotSplitAssignmentDoesNotLoadAllTableSchemas() {
        PostgresSourceConfig sourceConfig = createSourceConfig(StartupOptions.snapshot());
        PostgresPipelineRecordEmitter<Event> emitter = createEmitter(sourceConfig);

        assertThatCode(() -> emitter.applySplit(createSnapshotSplit())).doesNotThrowAnyException();
    }

    private static PostgresPipelineRecordEmitter<Event> createEmitter(
            PostgresSourceConfig sourceConfig) {
        return new PostgresPipelineRecordEmitter<>(
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL),
                null,
                sourceConfig,
                new PostgresOffsetFactory(),
                new FailingPostgresDialect(sourceConfig));
    }

    private static PostgresSourceConfig createSourceConfig(StartupOptions startupOptions) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("user");
        configFactory.password("password");
        configFactory.database("db0");
        configFactory.tableList("public.users");
        configFactory.startupOptions(startupOptions);
        return configFactory.create(0);
    }

    private static SnapshotSplit createSnapshotSplit() {
        return new SnapshotSplit(
                DBZ_TABLE_ID,
                "public.users:0",
                RowType.of(new IntType()),
                null,
                null,
                null,
                Collections.emptyMap());
    }

    private static class FailingPostgresDialect extends PostgresDialect {
        private FailingPostgresDialect(PostgresSourceConfig sourceConfig) {
            super(sourceConfig);
        }

        @Override
        public PostgresConnection openJdbcConnection() {
            throw new AssertionError("JDBC connection should be opened lazily.");
        }
    }
}
