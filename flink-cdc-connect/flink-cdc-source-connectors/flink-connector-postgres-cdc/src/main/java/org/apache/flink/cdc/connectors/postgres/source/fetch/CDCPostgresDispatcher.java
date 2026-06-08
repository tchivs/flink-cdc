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

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.schema.SchemaDispatcher;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresSchemaEventSuppressor;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTableIdRouter;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/** Postgres Dispatcher for cdc source with watermark. */
public class CDCPostgresDispatcher extends PostgresEventDispatcher<TableId>
        implements WatermarkDispatcher, SchemaDispatcher {
    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final PostgresTableIdRouter tableIdRouter;
    private final PostgresSchemaEventSuppressor schemaEventSuppressor;

    public CDCPostgresDispatcher(
            PostgresConnectorConfig connectorConfig,
            TopicSelector topicSelector,
            DatabaseSchema schema,
            ChangeEventQueue queue,
            DataCollectionFilters.DataCollectionFilter filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            HeartbeatFactory heartbeatFactory,
            SchemaNameAdjuster schemaNameAdjuster,
            PostgresTableIdRouter tableIdRouter) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                heartbeatFactory,
                schemaNameAdjuster);
        this.topic = topicSelector.getPrimaryTopic();
        this.queue = queue;
        this.tableIdRouter = tableIdRouter == null ? PostgresTableIdRouter.empty() : tableIdRouter;
        this.schemaEventSuppressor = new PostgresSchemaEventSuppressor();
    }

    @Override
    public void dispatchWatermarkEvent(
            Map<String, ?> sourcePartition,
            SourceSplitBase sourceSplit,
            Offset watermark,
            WatermarkKind watermarkKind)
            throws InterruptedException {
        SourceRecord sourceRecord =
                WatermarkEvent.create(
                        sourcePartition, topic, sourceSplit.splitId(), watermarkKind, watermark);
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    @Override
    public void dispatch(Table table) {
        Table routedTable = tableIdRouter.route(table);
        if (!schemaEventSuppressor.shouldDispatch(table, routedTable)) {
            return;
        }
        PostgresSchemaRecord schemaRecord = new PostgresSchemaRecord(routedTable);
        try {
            queue.enqueue(new DataChangeEvent(schemaRecord));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dispatchDataChangeEvent(
            PostgresPartition partition,
            TableId dataCollectionId,
            ChangeRecordEmitter<PostgresPartition> changeRecordEmitter)
            throws InterruptedException {
        return super.dispatchDataChangeEvent(
                partition,
                dataCollectionId,
                new RoutingChangeRecordEmitter(changeRecordEmitter, tableIdRouter));
    }

    private static class RoutingChangeRecordEmitter
            implements ChangeRecordEmitter<PostgresPartition> {

        private final ChangeRecordEmitter<PostgresPartition> delegate;
        private final PostgresTableIdRouter tableIdRouter;

        private RoutingChangeRecordEmitter(
                ChangeRecordEmitter<PostgresPartition> delegate,
                PostgresTableIdRouter tableIdRouter) {
            this.delegate = delegate;
            this.tableIdRouter =
                    tableIdRouter == null ? PostgresTableIdRouter.empty() : tableIdRouter;
        }

        @Override
        public void emitChangeRecords(
                DataCollectionSchema schema, Receiver<PostgresPartition> receiver)
                throws InterruptedException {
            delegate.emitChangeRecords(
                    schema, new RoutingChangeRecordReceiver(receiver, tableIdRouter));
        }

        @Override
        public PostgresPartition getPartition() {
            return delegate.getPartition();
        }

        @Override
        public OffsetContext getOffset() {
            return delegate.getOffset();
        }

        @Override
        public Envelope.Operation getOperation() {
            return delegate.getOperation();
        }
    }

    private static class RoutingChangeRecordReceiver
            implements ChangeRecordEmitter.Receiver<PostgresPartition> {

        private final ChangeRecordEmitter.Receiver<PostgresPartition> delegate;
        private final PostgresTableIdRouter tableIdRouter;

        private RoutingChangeRecordReceiver(
                ChangeRecordEmitter.Receiver<PostgresPartition> delegate,
                PostgresTableIdRouter tableIdRouter) {
            this.delegate = delegate;
            this.tableIdRouter =
                    tableIdRouter == null ? PostgresTableIdRouter.empty() : tableIdRouter;
        }

        @Override
        public void changeRecord(
                PostgresPartition partition,
                DataCollectionSchema schema,
                Envelope.Operation operation,
                Object key,
                Struct value,
                OffsetContext offset,
                ConnectHeaders headers)
                throws InterruptedException {
            tableIdRouter.rewriteSourceStruct(value);
            delegate.changeRecord(partition, schema, operation, key, value, offset, headers);
        }
    }
}
