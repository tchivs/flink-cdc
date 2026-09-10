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

import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for PostgreSQL Pipeline source metadata extraction. */
class PostgresEventDeserializerTest {

    private static final String UNSIGNED_LSN = "18446744073709551600";

    @Test
    void testStreamingMetadataValues() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("password", "must-not-be-exposed");
        offset.put(SourceInfo.TIMESTAMP_USEC_KEY, 1700000000123456L);
        offset.put(SourceInfo.TXID_KEY, 42L);
        offset.put("transaction_id", null);
        offset.put("lsn_proc", -16L);
        offset.put(SourceInfo.LSN_KEY, -16L);
        SourceRecord record =
                record("c", -16L, 42L, "[\"100\",\"-16\"]", "false", 1700000000123L, offset);

        Map<String, String> metadata = deserializer(pipelineMetadata()).metadata(record);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("source.op", "c");
        expected.put("source.database", "inventory");
        expected.put("source.schema", "public");
        expected.put("source.table", "orders");
        expected.put("source.lsn", UNSIGNED_LSN);
        expected.put("source.tx-id", "42");
        expected.put("source.sequence", "[\"100\",\"-16\"]");
        expected.put("source.snapshot", "false");
        expected.put("source.ts-ms", "1700000000123");
        expected.put("source.ts-us", "1700000000123456");
        expected.put("source.partition", "{\"server\":\"postgres-source\"}");
        expected.put(
                "source.offset",
                "{\"lsn\":18446744073709551600,\"lsn_proc\":18446744073709551600,\"transaction_id\":null,\"ts_usec\":1700000000123456,\"txId\":42}");
        assertThat(metadata).containsExactlyInAnyOrderEntriesOf(expected);
        assertThat(PostgreSQLReadableMetadata.SOURCE_LSN.getConverter().read(record).toString())
                .isEqualTo(UNSIGNED_LSN);
        assertThat(
                        PostgreSQLReadableMetadata.SOURCE_PARTITION
                                .getConverter()
                                .read(record)
                                .toString())
                .isEqualTo("{\"server\":\"postgres-source\"}");
    }

    @Test
    void testStreamingDmlOperationCodesRemainDistinct() {
        for (String operation : Arrays.asList("c", "u", "d")) {
            SourceRecord record =
                    record(
                            operation,
                            10L,
                            42L,
                            "[null,\"10\"]",
                            "false",
                            1700000000123L,
                            Collections.singletonMap(SourceInfo.LSN_KEY, 10L));

            assertThat(
                            deserializer(
                                            Collections.singletonList(
                                                    PostgreSQLReadableMetadata.SOURCE_OPERATION))
                                    .metadata(record))
                    .containsExactlyInAnyOrderEntriesOf(
                            Collections.singletonMap("source.op", operation));
        }
    }

    @Test
    void testSnapshotMetadataKeepsSentinelDistinctFromStreamingIdentity() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("transaction_id", null);
        offset.put(SourceInfo.LSN_KEY, 0L);
        offset.put(SourceInfo.TIMESTAMP_USEC_KEY, 1700000000000000L);
        SourceRecord record = record("r", 0L, null, "[null,\"0\"]", "false", 0L, offset);

        Map<String, String> metadata = deserializer(pipelineMetadata()).metadata(record);

        assertThat(metadata)
                .containsEntry("source.op", "r")
                .containsEntry("source.lsn", "0")
                .containsEntry("source.snapshot", "true")
                .containsEntry("source.sequence", "[null,\"0\"]")
                .doesNotContainKey("source.tx-id");
        assertThat(metadata.get("source.offset"))
                .isEqualTo("{\"lsn\":0,\"transaction_id\":null,\"ts_usec\":1700000000000000}");
    }

    @Test
    void testMissingTransactionAndLsnAreOmitted() {
        Map<String, Object> offset =
                Collections.singletonMap(SourceInfo.TIMESTAMP_USEC_KEY, 1700000000123456L);
        SourceRecord record = record("u", null, null, null, null, 1700000000123L, offset);

        Map<String, String> metadata = deserializer(pipelineMetadata()).metadata(record);

        assertThat(metadata)
                .doesNotContainKeys("source.lsn", "source.tx-id", "source.sequence")
                .containsEntry("source.snapshot", "false")
                .containsEntry("source.ts-us", "1700000000123456")
                .containsEntry("source.offset", "{\"ts_usec\":1700000000123456}");
    }

    @Test
    void testMetadataIsEmptyByDefault() {
        SourceRecord record =
                record(
                        "c",
                        10L,
                        42L,
                        "[null,\"10\"]",
                        "false",
                        1700000000123L,
                        Collections.singletonMap(SourceInfo.LSN_KEY, 10L));

        assertThat(deserializer(Collections.emptyList()).metadata(record)).isEmpty();
    }

    private static ExposedPostgresEventDeserializer deserializer(
            List<PostgreSQLReadableMetadata> metadata) {
        return new ExposedPostgresEventDeserializer(metadata);
    }

    private static List<PostgreSQLReadableMetadata> pipelineMetadata() {
        return Arrays.asList(
                PostgreSQLReadableMetadata.SOURCE_OPERATION,
                PostgreSQLReadableMetadata.SOURCE_DATABASE,
                PostgreSQLReadableMetadata.SOURCE_SCHEMA,
                PostgreSQLReadableMetadata.SOURCE_TABLE,
                PostgreSQLReadableMetadata.SOURCE_LSN,
                PostgreSQLReadableMetadata.SOURCE_TRANSACTION_ID,
                PostgreSQLReadableMetadata.SOURCE_SEQUENCE,
                PostgreSQLReadableMetadata.SOURCE_SNAPSHOT,
                PostgreSQLReadableMetadata.SOURCE_TIMESTAMP_MS,
                PostgreSQLReadableMetadata.SOURCE_TIMESTAMP_US,
                PostgreSQLReadableMetadata.SOURCE_PARTITION,
                PostgreSQLReadableMetadata.SOURCE_OFFSET);
    }

    private static SourceRecord record(
            String operation,
            Long lsn,
            Long transactionId,
            String sequence,
            String snapshot,
            long timestampMillis,
            Map<String, ?> offset) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                        .field(AbstractSourceInfo.SNAPSHOT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                        .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                        .build();
        Struct source =
                new Struct(sourceSchema)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, "inventory")
                        .put(AbstractSourceInfo.SCHEMA_NAME_KEY, "public")
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, "orders")
                        .put(AbstractSourceInfo.TIMESTAMP_KEY, timestampMillis)
                        .put(AbstractSourceInfo.SNAPSHOT_KEY, snapshot)
                        .put(AbstractSourceInfo.SEQUENCE_KEY, sequence)
                        .put(SourceInfo.LSN_KEY, lsn)
                        .put(SourceInfo.TXID_KEY, transactionId);
        Schema envelopeSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct envelope =
                new Struct(envelopeSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, operation);
        return new SourceRecord(
                Collections.singletonMap("server", "postgres-source"),
                offset,
                "routed.target.table",
                null,
                null,
                envelopeSchema,
                envelope);
    }

    private static final class ExposedPostgresEventDeserializer extends PostgresEventDeserializer {

        private ExposedPostgresEventDeserializer(
                List<PostgreSQLReadableMetadata> readableMetadata) {
            super(DebeziumChangelogMode.ALL, readableMetadata);
        }

        private Map<String, String> metadata(SourceRecord record) {
            return getMetadata(record);
        }
    }
}
