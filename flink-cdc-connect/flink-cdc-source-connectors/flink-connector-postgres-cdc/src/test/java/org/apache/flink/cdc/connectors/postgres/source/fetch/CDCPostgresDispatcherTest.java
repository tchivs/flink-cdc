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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionRoutingState;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTableIdRouter;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.LoggingContext;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CDCPostgresDispatcher}. */
class CDCPostgresDispatcherTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD = new TableId(null, "public", "orders_2025");

    @Test
    void rewritesStreamingSourceStructAfterChildSchemaLookup() throws Exception {
        PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(defaultConnectorConfig());
        TopicSelector<TableId> topicSelector =
                TopicSelector.defaultSelector(
                        connectorConfig,
                        (id, prefix, delimiter) ->
                                String.join(delimiter, prefix, id.schema(), id.table()));
        ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(10))
                        .maxBatchSize(10)
                        .maxQueueSize(10)
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("postgres", "test", "test"))
                        .build();
        TestDatabaseSchema databaseSchema = new TestDatabaseSchema(CHILD);
        PostgresTableIdRouter tableIdRouter =
                PostgresTableIdRouter.of(
                        () ->
                                PartitionRoutingState.of(
                                        Collections.singletonMap(
                                                PARENT, Collections.singletonList(CHILD))));

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        id -> true,
                        DataChangeEvent::new,
                        NoopEventMetadataProvider.INSTANCE,
                        new HeartbeatFactory<>(
                                connectorConfig, topicSelector, SchemaNameAdjuster.NO_OP),
                        SchemaNameAdjuster.NO_OP,
                        tableIdRouter);

        boolean dispatched =
                dispatcher.dispatchDataChangeEvent(
                        new PostgresPartition("postgres"),
                        CHILD,
                        new TestChangeRecordEmitter(CHILD));

        DataChangeEvent event = queue.poll().get(0);
        SourceRecord sourceRecord = event.getRecord();
        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct(SOURCE);

        assertThat(dispatched).isTrue();
        assertThat(databaseSchema.requestedTableId()).isEqualTo(CHILD);
        assertThat(sourceRecord.topic()).isEqualTo("test_server.public.orders_2025");
        assertThat(source.getString(SCHEMA_NAME_KEY)).isEqualTo(PARENT.schema());
        assertThat(source.getString(TABLE_NAME_KEY)).isEqualTo(PARENT.table());
    }

    private static Configuration defaultConnectorConfig() {
        return TestHelper.defaultConfig()
                .with("table.include.list", "public.orders_2025")
                .with("tombstones.on.delete", false)
                .build();
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field(SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    private static Struct sourceStruct(TableId tableId) {
        return new Struct(sourceSchema())
                .put(SCHEMA_NAME_KEY, tableId.schema())
                .put(TABLE_NAME_KEY, tableId.table());
    }

    private static class TestChangeRecordEmitter implements ChangeRecordEmitter<PostgresPartition> {

        private final TableId physicalTableId;
        private final TestOffsetContext offsetContext = new TestOffsetContext();

        private TestChangeRecordEmitter(TableId physicalTableId) {
            this.physicalTableId = physicalTableId;
        }

        @Override
        public void emitChangeRecords(
                DataCollectionSchema schema, Receiver<PostgresPartition> receiver)
                throws InterruptedException {
            Schema valueSchema = schema.getEnvelopeSchema().schema();
            Struct value = new Struct(valueSchema).put(SOURCE, sourceStruct(physicalTableId));
            receiver.changeRecord(
                    getPartition(),
                    schema,
                    getOperation(),
                    null,
                    value,
                    offsetContext,
                    new ConnectHeaders());
        }

        @Override
        public PostgresPartition getPartition() {
            return new PostgresPartition("postgres");
        }

        @Override
        public OffsetContext getOffset() {
            return offsetContext;
        }

        @Override
        public Envelope.Operation getOperation() {
            return Envelope.Operation.CREATE;
        }
    }

    private static class TestDatabaseSchema implements DatabaseSchema<TableId> {

        private final TableId tableIdWithSchema;
        private final TestDataCollectionSchema tableSchema;
        private TableId requestedTableId;

        private TestDatabaseSchema(TableId tableIdWithSchema) {
            this.tableIdWithSchema = tableIdWithSchema;
            this.tableSchema = new TestDataCollectionSchema(tableIdWithSchema);
        }

        @Override
        public DataCollectionSchema schemaFor(TableId id) {
            requestedTableId = id;
            return tableIdWithSchema.equals(id) ? tableSchema : null;
        }

        private TableId requestedTableId() {
            return requestedTableId;
        }

        @Override
        public boolean tableInformationComplete() {
            return true;
        }

        @Override
        public boolean isHistorized() {
            return false;
        }

        @Override
        public void close() {}
    }

    private static class TestDataCollectionSchema implements DataCollectionSchema {

        private final TableId tableId;
        private final Envelope envelope;

        private TestDataCollectionSchema(TableId tableId) {
            this.tableId = tableId;
            this.envelope =
                    Envelope.defineSchema()
                            .withName("test.Envelope")
                            .withSource(sourceSchema())
                            .withSchema(optionalRowSchema(), Envelope.FieldName.BEFORE)
                            .withSchema(optionalRowSchema(), Envelope.FieldName.AFTER)
                            .build();
        }

        @Override
        public DataCollectionId id() {
            return tableId;
        }

        @Override
        public Schema keySchema() {
            return null;
        }

        @Override
        public Envelope getEnvelopeSchema() {
            return envelope;
        }
    }

    private static Schema optionalRowSchema() {
        return SchemaBuilder.struct().optional().build();
    }

    private static class TestOffsetContext implements OffsetContext {

        private final TransactionContext transactionContext = new TransactionContext();

        @Override
        public Map<String, ?> getOffset() {
            return Collections.singletonMap("lsn", 1L);
        }

        @Override
        public Schema getSourceInfoSchema() {
            return sourceSchema();
        }

        @Override
        public Struct getSourceInfo() {
            return sourceStruct(CHILD);
        }

        @Override
        public boolean isSnapshotRunning() {
            return false;
        }

        @Override
        public void markLastSnapshotRecord() {}

        @Override
        public void preSnapshotStart() {}

        @Override
        public void preSnapshotCompletion() {}

        @Override
        public void postSnapshotCompletion() {}

        @Override
        public void event(DataCollectionId dataCollectionId, Instant instant) {}

        @Override
        public TransactionContext getTransactionContext() {
            return transactionContext;
        }

        @Override
        public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
            return null;
        }
    }

    private enum NoopEventMetadataProvider implements EventMetadataProvider {
        INSTANCE;

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return null;
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return Collections.emptyMap();
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return null;
        }
    }
}
