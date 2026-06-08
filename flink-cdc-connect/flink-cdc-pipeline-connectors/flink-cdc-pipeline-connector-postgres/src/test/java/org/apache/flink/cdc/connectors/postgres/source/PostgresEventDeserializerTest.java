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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresEventDeserializer}. */
class PostgresEventDeserializerTest {

    @Test
    void usesSourceStructTableIdBeforeTopicTableId() {
        TestDeserializer deserializer =
                new TestDeserializer(DebeziumChangelogMode.ALL, true, "inventory");
        SourceRecord record = sourceRecord("server.public.orders_2025", "public", "orders_parent");

        assertThat(deserializer.tableId(record))
                .isEqualTo(TableId.tableId("inventory", "public", "orders_parent"));
    }

    @Test
    void fallsBackToTopicTableIdWhenSourceStructHasNoTable() {
        TestDeserializer deserializer =
                new TestDeserializer(DebeziumChangelogMode.ALL, false, null);
        Schema valueSchema = valueSchemaWithoutTable();
        Struct value =
                new Struct(valueSchema)
                        .put(
                                Envelope.FieldName.SOURCE,
                                new Struct(valueSchema.field(Envelope.FieldName.SOURCE).schema()));
        SourceRecord record = sourceRecord("server.public.orders_2025", valueSchema, value);

        assertThat(deserializer.tableId(record))
                .isEqualTo(TableId.tableId("public", "orders_2025"));
    }

    private static SourceRecord sourceRecord(String topic, String schemaName, String tableName) {
        Schema sourceSchema = sourceSchema();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct source =
                new Struct(sourceSchema)
                        .put(AbstractSourceInfo.SCHEMA_NAME_KEY, schemaName)
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, tableName);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, Envelope.Operation.READ.code());

        return sourceRecord(topic, valueSchema, value);
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    private static Schema valueSchemaWithoutTable() {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        return SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, sourceSchema).build();
    }

    private static SourceRecord sourceRecord(String topic, Schema valueSchema, Struct value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                topic,
                null,
                null,
                null,
                valueSchema,
                value);
    }

    private static class TestDeserializer extends PostgresEventDeserializer {

        private TestDeserializer(
                DebeziumChangelogMode changelogMode,
                boolean includeDatabaseInTableId,
                String databaseName) {
            super(changelogMode, Collections.emptyList(), includeDatabaseInTableId, databaseName);
        }

        private TableId tableId(SourceRecord record) {
            return getTableId(record);
        }
    }
}
