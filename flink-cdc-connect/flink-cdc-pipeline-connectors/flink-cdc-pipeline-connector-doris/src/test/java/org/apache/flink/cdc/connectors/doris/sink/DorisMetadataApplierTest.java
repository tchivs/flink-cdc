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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SCHEMA_CHANGE_ALLOWED_TYPES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_BUCKETS;

/** Tests for {@link DorisMetadataApplier}. */
class DorisMetadataApplierTest {

    private static final TableId TABLE_ID = TableId.parse("test_database.test_table");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testDefaultTableSchemaKeepsAutomaticBuckets() {
        TableSchema tableSchema =
                createMetadataApplier(new Configuration())
                        .buildTableSchema(new CreateTableEvent(TABLE_ID, SCHEMA));
        Assertions.assertThat(tableSchema.getFields().keySet()).containsExactly("id", "payload");

        Assertions.assertThat(tableSchema.getTableBuckets()).isNull();
        Assertions.assertThat(DorisSchemaFactory.generateCreateTableDDL(tableSchema))
                .contains("`id` BIGINT NOT NULL")
                .contains("`payload` STRING")
                .doesNotContain("`payload` STRING NOT NULL")
                .contains("BUCKETS AUTO");
    }

    @Test
    void testExplicitBucketsReachGeneratedTableSchema() {
        Configuration config = new Configuration().set(TABLE_CREATE_BUCKETS, 8);
        TableSchema tableSchema =
                createMetadataApplier(config)
                        .buildTableSchema(new CreateTableEvent(TABLE_ID, SCHEMA));

        Assertions.assertThat(tableSchema.getTableBuckets()).isEqualTo(8);
        Assertions.assertThat(DorisSchemaFactory.generateCreateTableDDL(tableSchema))
                .contains("BUCKETS 8")
                .doesNotContain("BUCKETS AUTO");
    }

    @Test
    void testFrameworkAllowlistIsIntersectedWithConnectorAllowlist() {
        Configuration config =
                Configuration.fromMap(
                        Collections.singletonMap(SCHEMA_CHANGE_ALLOWED_TYPES.key(), "ADD_COLUMN"));
        DorisMetadataApplier metadataApplier = createMetadataApplier(config);
        metadataApplier.setAcceptedSchemaEvolutionTypes(EnumSet.allOf(SchemaChangeEventType.class));

        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(CREATE_TABLE)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ADD_COLUMN)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(DROP_COLUMN)).isFalse();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(RENAME_COLUMN)).isFalse();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ALTER_COLUMN_TYPE))
                .isFalse();

        metadataApplier.setAcceptedSchemaEvolutionTypes(Collections.emptySet());
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(CREATE_TABLE)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ADD_COLUMN)).isFalse();
    }

    @Test
    void testDirectApplyRejectsDisallowedDropRenameAndAlter() {
        Configuration config =
                Configuration.fromMap(
                        Collections.singletonMap(SCHEMA_CHANGE_ALLOWED_TYPES.key(), "ADD_COLUMN"));
        DorisMetadataApplier metadataApplier = createMetadataApplier(config);
        metadataApplier.setAcceptedSchemaEvolutionTypes(EnumSet.allOf(SchemaChangeEventType.class));

        List<SchemaChangeEvent> disallowedEvents =
                Arrays.asList(
                        new DropColumnEvent(TABLE_ID, Collections.singletonList("payload")),
                        new RenameColumnEvent(
                                TABLE_ID, Collections.singletonMap("payload", "new_payload")),
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("payload", DataTypes.VARCHAR(128))));

        for (SchemaChangeEvent event : disallowedEvents) {
            Assertions.assertThatThrownBy(() -> metadataApplier.applySchemaChange(event))
                    .isInstanceOf(SchemaEvolveException.class)
                    .satisfies(
                            throwable ->
                                    Assertions.assertThat(
                                                    ((SchemaEvolveException) throwable)
                                                            .getExceptionMessage())
                                            .contains(event.getType().name())
                                            .contains(SCHEMA_CHANGE_ALLOWED_TYPES.key()));
        }
    }

    @Test
    void testVisibleColumnsAndSequencePropertyCoexistInDdl() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.delete-mode", "VISIBLE");
        options.put("sink.visible-delete-column", "deleted");
        options.put("sink.metadata-columns.source_lsn.source", "source.lsn");
        options.put("sink.metadata-columns.source_lsn.type", "BIGINT");
        options.put("sink.metadata-columns.source_snapshot.source", "source.snapshot");
        options.put("sink.metadata-columns.source_snapshot.type", "BOOLEAN");
        options.put("sink.metadata-columns.source_snapshot.default", "false");
        options.put("table.create.properties.function_column.sequence_col", "source_lsn");

        TableSchema tableSchema =
                createMetadataApplier(Configuration.fromMap(options))
                        .buildTableSchema(new CreateTableEvent(TABLE_ID, SCHEMA));
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);

        Assertions.assertThat(tableSchema.getFields().keySet())
                .containsExactly("id", "payload", "source_lsn", "source_snapshot", "deleted");
        Assertions.assertThat(ddl)
                .contains("`source_lsn` BIGINT NOT NULL")
                .contains("`source_snapshot` BOOLEAN NOT NULL DEFAULT 'false'")
                .contains("`deleted` BOOLEAN NOT NULL DEFAULT 'false'")
                .contains("'function_column.sequence_col'='source_lsn'");
    }

    @Test
    void testStringMetadataDefaultIsRenderedAsOneLiteral() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.metadata-columns.metadata_text.source", "source.note' COMMENT 'injected");
        options.put("sink.metadata-columns.metadata_text.type", "STRING");
        options.put("sink.metadata-columns.metadata_text.default", "x' COMMENT 'injected");

        String ddl =
                DorisSchemaFactory.generateCreateTableDDL(
                        createMetadataApplier(Configuration.fromMap(options))
                                .buildTableSchema(new CreateTableEvent(TABLE_ID, SCHEMA)));

        Assertions.assertThat(ddl)
                .contains(
                        "`metadata_text` STRING NOT NULL DEFAULT 'x'' COMMENT ''injected' COMMENT 'Flink CDC event metadata'")
                .doesNotContain("source.note' COMMENT 'injected");
    }

    @Test
    void testCreateAndAddColumnsCannotCollideWithVisibleColumns() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.delete-mode", "VISIBLE");
        options.put("sink.visible-delete-column", "deleted");
        options.put("sink.metadata-columns.source_lsn.source", "source.lsn");
        options.put("sink.metadata-columns.source_lsn.type", "BIGINT");
        DorisMetadataApplier metadataApplier =
                createMetadataApplier(Configuration.fromMap(options));

        Schema hiddenDeleteSignSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("__DORIS_DELETE_SIGN__", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.buildTableSchema(
                                        new CreateTableEvent(TABLE_ID, hiddenDeleteSignSchema)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved by Doris");

        Schema collidingSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("SOURCE_LSN", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Assertions.assertThatThrownBy(
                        () ->
                                metadataApplier.buildTableSchema(
                                        new CreateTableEvent(TABLE_ID, collidingSchema)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collides");

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("DELETED", DataTypes.BOOLEAN()))));
        Assertions.assertThatThrownBy(() -> metadataApplier.applySchemaChange(addColumnEvent))
                .isInstanceOf(SchemaEvolveException.class)
                .hasMessageContaining("collides");
    }

    private static DorisMetadataApplier createMetadataApplier(Configuration config) {
        DorisOptions dorisOptions =
                DorisOptions.builder().setFenodes("127.0.0.1:8030").setUsername("root").build();
        return new DorisMetadataApplier(dorisOptions, config);
    }
}
