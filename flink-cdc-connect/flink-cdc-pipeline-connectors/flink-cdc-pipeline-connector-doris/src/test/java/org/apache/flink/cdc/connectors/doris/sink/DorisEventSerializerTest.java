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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE;

/** A test for {@link org.apache.flink.cdc.connectors.doris.sink.DorisEventSerializer} . */
public class DorisEventSerializerTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private static DorisEventSerializer dorisEventSerializer;

    private static final TableId TABLE_ID = TableId.parse("doris_database.doris_table");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("create_date", DataTypes.DATE())
                    .physicalColumn("create_time", DataTypes.TIMESTAMP())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator RECORD_DATA_GENERATOR =
            new BinaryRecordDataGenerator(((RowType) SCHEMA.toRowDataType()));
    private static final Schema ODS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator ODS_RECORD_DATA_GENERATOR =
            new BinaryRecordDataGenerator(((RowType) ODS_SCHEMA.toRowDataType()));

    @Test
    public void testDataChangeEventWithTimeDataType() throws IOException {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id_", DataTypes.BIGINT().notNull())
                        .physicalColumn("time_0_", DataTypes.TIME(0))
                        .physicalColumn("time_3_", DataTypes.TIME(3))
                        .primaryKey("id_")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, schema);
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    1L,
                                    TimeData.fromLocalTime(LocalTime.of(19, 43, 17)),
                                    TimeData.fromLocalTime(LocalTime.of(21, 45, 3, 123000000)),
                                }));

        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        dorisEventSerializer.serialize(createTableEvent);
        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());

        Assertions.assertThat(jsonNode.get("id_").asLong()).isEqualTo(1L);
        Assertions.assertThat(jsonNode.get("time_0_").asText()).isEqualTo("19:43:17");
        Assertions.assertThat(jsonNode.get("time_3_").asText()).isEqualTo("21:45:03.123");
    }

    @Test
    public void testDataChangeEventWithDateTimePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.parse("2025-01-16T08:00:00Z"), ZoneId.of("Z"));
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo("2025-01-16 08:00:00.000000");
    }

    @Test
    public void testDataChangeEventIfDatetimePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATETIME);
    }

    @Test
    public void testConfigurationIsSnapshottedAtConstruction() throws IOException {
        Configuration config = new Configuration();
        DorisEventSerializer serializer = new DorisEventSerializer(ZoneId.of("UTC"), config);
        Map<String, String> latePartitionOptions = new HashMap<>();
        latePartitionOptions.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        latePartitionOptions.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        latePartitionOptions.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");
        config.addAll(Configuration.fromMap(latePartitionOptions));
        serializer.serialize(new CreateTableEvent(TABLE_ID, SCHEMA));

        JsonNode row =
                serializeJson(
                        serializer,
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                RECORD_DATA_GENERATOR.generate(
                                        new Object[] {
                                            new BinaryStringData("1"),
                                            new BinaryStringData("flink"),
                                            DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                            null,
                                        })));

        Assertions.assertThat(row.get("create_time").isNull()).isTrue();
    }

    @Test
    public void testDataChangeEventWithDatePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText()).isEqualTo("2025-01-16");
    }

    @Test
    public void testDataChangeEventIfDatePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    null,
                                    TimestampData.fromMillis(System.currentTimeMillis()),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATE);
    }

    @Test
    public void testPhysicalDeleteSerializationIsUnchangedByDefault() throws IOException {
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), new Configuration());
        serializer.serialize(new CreateTableEvent(TABLE_ID, ODS_SCHEMA));

        JsonNode insert =
                serializeJson(
                        serializer,
                        DataChangeEvent.insertEvent(TABLE_ID, odsRecord(1L, "current")));
        JsonNode delete =
                serializeJson(
                        serializer,
                        DataChangeEvent.deleteEvent(TABLE_ID, odsRecord(1L, "current")));
        Assertions.assertThat(insert.size()).isEqualTo(3);
        Assertions.assertThat(insert.get("__DORIS_DELETE_SIGN__").asText()).isEqualTo("0");
        Assertions.assertThat(delete.size()).isEqualTo(3);
        Assertions.assertThat(delete.get("payload").asText()).isEqualTo("current");
        Assertions.assertThat(delete.get("__DORIS_DELETE_SIGN__").asText()).isEqualTo("1");
    }

    @Test
    public void testVisibleInsertUpdateAndDeleteWithMetadata() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("sink.delete-mode", "VISIBLE");
        options.put("sink.visible-delete-column", "deleted");
        options.put("sink.metadata-columns.source_lsn.source", "source.lsn");
        options.put("sink.metadata-columns.source_lsn.type", "BIGINT");
        options.put("sink.metadata-columns.snapshot.source", "source.snapshot");
        options.put("sink.metadata-columns.snapshot.type", "BOOLEAN");
        options.put("sink.metadata-columns.snapshot.default", "false");
        options.put("sink.metadata-columns.source_offset.source", "source.offset");
        options.put("sink.metadata-columns.source_offset.type", "STRING");
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), Configuration.fromMap(options));
        serializer.serialize(new CreateTableEvent(TABLE_ID, ODS_SCHEMA));

        Map<String, String> insertMetadata = new HashMap<>();
        insertMetadata.put("source.lsn", "42");
        insertMetadata.put("source.snapshot", "true");
        insertMetadata.put("source.offset", "  exact offset  ");
        JsonNode insert =
                serializeJson(
                        serializer,
                        DataChangeEvent.insertEvent(
                                TABLE_ID, odsRecord(1L, "inserted"), insertMetadata));

        Map<String, String> updateMetadata = new HashMap<>();
        updateMetadata.put("source.lsn", "43");
        updateMetadata.put("source.offset", "update-offset");
        JsonNode update =
                serializeJson(
                        serializer,
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                odsRecord(1L, "inserted"),
                                odsRecord(1L, "updated"),
                                updateMetadata));

        Map<String, String> deleteMetadata = new HashMap<>();
        deleteMetadata.put("source.lsn", "44");
        deleteMetadata.put("source.snapshot", "false");
        deleteMetadata.put("source.offset", "delete-offset");
        JsonNode delete =
                serializeJson(
                        serializer,
                        DataChangeEvent.deleteEvent(
                                TABLE_ID, odsRecord(1L, "before-delete"), deleteMetadata));

        Assertions.assertThat(insert.get("deleted").asBoolean()).isFalse();
        Assertions.assertThat(insert.get("source_lsn").isIntegralNumber()).isTrue();
        Assertions.assertThat(insert.get("source_lsn").asLong()).isEqualTo(42L);
        Assertions.assertThat(insert.get("snapshot").asBoolean()).isTrue();
        Assertions.assertThat(insert.get("source_offset").asText()).isEqualTo("  exact offset  ");
        Assertions.assertThat(update.get("payload").asText()).isEqualTo("updated");
        Assertions.assertThat(update.get("deleted").asBoolean()).isFalse();
        Assertions.assertThat(update.get("snapshot").asBoolean()).isFalse();
        Assertions.assertThat(delete.get("payload").asText()).isEqualTo("before-delete");
        Assertions.assertThat(delete.get("deleted").asBoolean()).isTrue();
        Assertions.assertThat(delete.has("__DORIS_DELETE_SIGN__")).isFalse();
    }

    @Test
    public void testMetadataConversionFailsClosed() throws IOException {
        Map<String, String> bigintOptions = new HashMap<>();
        bigintOptions.put("sink.metadata-columns.source_lsn.source", "source.lsn");
        bigintOptions.put("sink.metadata-columns.source_lsn.type", "BIGINT");
        bigintOptions.put("table.create.properties.function_column.sequence_col", "source_lsn");
        DorisEventSerializer bigintSerializer =
                new DorisEventSerializer(ZoneId.of("UTC"), Configuration.fromMap(bigintOptions));
        bigintSerializer.serialize(new CreateTableEvent(TABLE_ID, ODS_SCHEMA));

        JsonNode snapshot =
                serializeJson(
                        bigintSerializer,
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                odsRecord(1L, "snapshot"),
                                Collections.singletonMap("source.lsn", "0")));
        Assertions.assertThat(snapshot.get("source_lsn").asLong()).isZero();

        Assertions.assertThatThrownBy(
                        () ->
                                bigintSerializer.serialize(
                                        DataChangeEvent.insertEvent(
                                                TABLE_ID, odsRecord(1L, "missing"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("source.lsn")
                .hasMessageContaining("missing");
        for (String invalidBigint :
                Arrays.asList("", "-1", "+1", "01", "-0", "9223372036854775808")) {
            Assertions.assertThatThrownBy(
                            () ->
                                    bigintSerializer.serialize(
                                            DataChangeEvent.insertEvent(
                                                    TABLE_ID,
                                                    odsRecord(1L, "invalid"),
                                                    Collections.singletonMap(
                                                            "source.lsn", invalidBigint))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("source_lsn");
        }

        Map<String, String> booleanOptions = new HashMap<>();
        booleanOptions.put("sink.metadata-columns.snapshot.source", "source.snapshot");
        booleanOptions.put("sink.metadata-columns.snapshot.type", "BOOLEAN");
        DorisEventSerializer booleanSerializer =
                new DorisEventSerializer(ZoneId.of("UTC"), Configuration.fromMap(booleanOptions));
        booleanSerializer.serialize(new CreateTableEvent(TABLE_ID, ODS_SCHEMA));
        Assertions.assertThatThrownBy(
                        () ->
                                booleanSerializer.serialize(
                                        DataChangeEvent.insertEvent(
                                                TABLE_ID,
                                                odsRecord(1L, "invalid"),
                                                Collections.singletonMap(
                                                        "source.snapshot", "TRUE"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("canonical true or false");
    }

    @Test
    public void testSerializerRejectsCreateAndAddColumnCollisions() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("sink.delete-mode", "VISIBLE");
        options.put("sink.visible-delete-column", "deleted");
        DorisEventSerializer serializer =
                new DorisEventSerializer(ZoneId.of("UTC"), Configuration.fromMap(options));
        Schema collidingSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("DELETED", DataTypes.BOOLEAN())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () -> serializer.serialize(new CreateTableEvent(TABLE_ID, collidingSchema)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collides");

        serializer.serialize(new CreateTableEvent(TABLE_ID, ODS_SCHEMA));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("deleted", DataTypes.BOOLEAN()))));
        Assertions.assertThatThrownBy(() -> serializer.serialize(addColumnEvent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collides");
    }

    private JsonNode serializeJson(DorisEventSerializer serializer, DataChangeEvent event)
            throws IOException {
        return objectMapper.readTree(serializer.serialize(event).getRow());
    }

    private static org.apache.flink.cdc.common.data.RecordData odsRecord(long id, String payload) {
        return ODS_RECORD_DATA_GENERATOR.generate(new Object[] {id, new BinaryStringData(payload)});
    }
}
