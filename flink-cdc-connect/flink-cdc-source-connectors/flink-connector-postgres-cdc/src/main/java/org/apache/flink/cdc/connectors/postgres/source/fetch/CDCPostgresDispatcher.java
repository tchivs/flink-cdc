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
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.schema.SchemaDispatcher;
import org.apache.flink.cdc.connectors.postgres.source.utils.SchemaFingerprints;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Postgres Dispatcher for cdc source with watermark and partition routing. */
public class CDCPostgresDispatcher extends PostgresEventDispatcher<TableId>
        implements WatermarkDispatcher, SchemaDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(CDCPostgresDispatcher.class);

    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final Map<TableId, TableId> childToParentMapping;
    private final DatabaseSchema<?> schemaRef;

    /**
     * Tracks the last dispatched schema fingerprint per (routed) table to avoid emitting duplicate
     * SchemaChangeEvents when the schema has not actually changed. Uses {@link
     * SchemaFingerprints#computeSchemaFingerprint(Table)} for collision-resistant fingerprinting.
     */
    private final Map<TableId, Integer> lastDispatchedSchemaFingerprint = new HashMap<>();

    /** Tracks parent TableIds whose schema has already been registered (avoid repeated work). */
    private final Set<TableId> registeredParentSchemas = new HashSet<>();

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
            Map<TableId, TableId> childToParentMapping) {
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
        this.schemaRef = schema;
        this.childToParentMapping =
                childToParentMapping == null ? Collections.emptyMap() : childToParentMapping;
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
        Table routed = routeTable(table);
        TableId routedId = routed.id();
        int fingerprint = SchemaFingerprints.computeSchemaFingerprint(routed);
        Integer lastFingerprint = lastDispatchedSchemaFingerprint.get(routedId);
        if (lastFingerprint != null && lastFingerprint == fingerprint) {
            // Schema unchanged for this routed table — skip redundant event
            return;
        }
        lastDispatchedSchemaFingerprint.put(routedId, fingerprint);

        PostgresSchemaRecord schemaRecord = new PostgresSchemaRecord(routed);
        try {
            queue.enqueue(new DataChangeEvent(schemaRecord));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dispatchDataChangeEvent(
            io.debezium.connector.postgresql.PostgresPartition partition,
            TableId dataCollectionId,
            ChangeRecordEmitter<io.debezium.connector.postgresql.PostgresPartition>
                    changeRecordEmitter)
            throws InterruptedException {
        TableId routed = routeTableId(dataCollectionId);
        if (!routed.equals(dataCollectionId)) {
            ensureParentSchemaRegistered(dataCollectionId, routed);
        }
        return super.dispatchDataChangeEvent(partition, routed, changeRecordEmitter);
    }

    /**
     * Ensures the parent table's schema is registered in the Debezium schema registry.
     *
     * <p>When {@code publish_via_partition_root=false} (PG10-compatible mode), PostgreSQL sends WAL
     * events tagged with child partition TableIds and only sends Relation messages for child
     * tables. The parent table never receives a Relation message, so its schema is never
     * registered. This method synthesizes the parent table schema from the child's schema on first
     * encounter, enabling Debezium's EventDispatcher to find the schema when processing routed
     * events.
     */
    private void ensureParentSchemaRegistered(TableId childId, TableId parentId) {
        if (registeredParentSchemas.contains(parentId)) {
            return;
        }
        if (schemaRef instanceof RelationAwarePostgresSchema) {
            RelationAwarePostgresSchema pgSchema = (RelationAwarePostgresSchema) schemaRef;
            if (pgSchema.registerParentTableFromChild(childId, parentId)) {
                LOG.info(
                        "Registered parent table schema '{}' from child '{}' "
                                + "(publish_via_partition_root=false mode)",
                        parentId,
                        childId);
            }
            registeredParentSchemas.add(parentId);
        }
    }

    private TableId routeTableId(TableId tableId) {
        if (tableId == null || childToParentMapping.isEmpty()) {
            return tableId;
        }
        return childToParentMapping.getOrDefault(tableId, tableId);
    }

    private Table routeTable(Table table) {
        if (table == null) {
            return null;
        }
        if (childToParentMapping.isEmpty()) {
            return table;
        }
        TableId parentTableId = routeTableId(table.id());
        if (parentTableId == null || parentTableId.equals(table.id())) {
            return table;
        }
        return table.edit().tableId(parentTableId).create();
    }
}
