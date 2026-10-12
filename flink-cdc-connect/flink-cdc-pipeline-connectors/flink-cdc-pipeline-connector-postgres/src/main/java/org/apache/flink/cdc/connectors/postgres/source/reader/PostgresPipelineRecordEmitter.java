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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresPartitionRouting;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils.toCdcTableId;
import static org.apache.flink.cdc.connectors.postgres.utils.SchemaChangeUtil.inferSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.postgres.utils.SchemaChangeUtil.toCreateTableEvent;

/** The {@link RecordEmitter} implementation for PostgreSQL pipeline connector. */
public class PostgresPipelineRecordEmitter<T> extends PostgresSourceRecordEmitter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPipelineRecordEmitter.class);
    private final PostgresSourceConfig sourceConfig;
    private final PostgresDialect postgresDialect;

    // Used when startup mode is initial
    private final Set<TableId> alreadySendCreateTableTables;
    private final boolean isBounded;
    private final boolean includeDatabaseInTableId;
    private final Map<TableId, CreateTableEvent> createTableEventCache;

    /** Schema of each routed partition root as evolved by the column changes emitted for it. */
    private final Map<TableId, Schema> parentSchemas = new HashMap<>();

    // Used when startup mode is not initial
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;

    public PostgresPipelineRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            PostgresSourceConfig sourceConfig,
            OffsetFactory offsetFactory,
            PostgresDialect postgresDialect) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        this.sourceConfig = sourceConfig;
        this.includeDatabaseInTableId = sourceConfig.isIncludeDatabaseInTableId();
        this.postgresDialect = postgresDialect;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
        generateCreateTableEvent(sourceConfig);
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    public void applySplit(SourceSplitBase split) {
        if ((isBounded) && createTableEventCache.isEmpty() && split instanceof SnapshotSplit) {
            // TableSchemas in SnapshotSplit only contains one table.
            createTableEventCache.putAll(generateCreateTableEvent(sourceConfig));
        } else {
            for (Map.Entry<TableId, TableChanges.TableChange> entry :
                    split.getTableSchemas().entrySet()) {
                TableChanges.TableChange tableChange = entry.getValue();

                Table table = tableChange.getTable();
                CreateTableEvent createTableEvent =
                        toCreateTableEvent(table, sourceConfig, postgresDialect);
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .applyChangeEvent(createTableEvent);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            // In snapshot mode, we simply emit all schemas at once.
            createTableEventCache.forEach(
                    (tableId, createTableEvent) -> {
                        output.collect((T) createTableEvent);
                    });
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            maybeSendCreateTableEventFromCache(tableId, output);
        } else if (isDataChangeRecord(element)) {
            // Captured before routing rewrites the source block to the partition root.
            TableId originTableId = getTableId(element);
            element = routeToParentTable(element);
            ensureCreateTableEventSent(getTableId(element), originTableId, output);
        } else if (isSchemaChangeEvent(element) && sourceConfig.isIncludeSchemaChanges()) {
            handleSchemaChangeRecord(element, output, splitState);
        }
        super.processElement(element, output, splitState);
    }

    private void ensureCreateTableEventSent(
            TableId tableId, @Nullable TableId originChild, SourceOutput<T> output) {
        maybeSendCreateTableEventFromCache(tableId, output);
        // In rare case, we may miss some CreateTableEvents before DataChangeEvents.
        // Don't send CreateTableEvent for SchemaChangeEvents as it's the latest schema.
        if (!createTableEventCache.containsKey(tableId)) {
            CreateTableEvent createTableEvent = getCreateTableEvent(tableId, originChild);
            sendCreateTableEvent(createTableEvent, output);
            createTableEventCache.put(tableId, createTableEvent);
        }
    }

    private void handleSchemaChangeRecord(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState) {
        if (!(element instanceof PostgresSchemaRecord)) {
            // Ignore non-Postgres schema change records; they may represent non-relation
            // schema changes that are not handled via PostgresSchemaRecord.
            LOG.warn("Ignoring non-PostgresSchemaRecord schema change event: {}", element);
            return;
        }
        Map<TableId, TableChanges.TableChange> existedTableSchemas =
                splitState.toSourceSplit().getTableSchemas();
        PostgresSchemaRecord schemaRecord = (PostgresSchemaRecord) element;
        Table schemaAfter = schemaRecord.getTable();
        Table schemaBefore = null;
        if (existedTableSchemas.containsKey(schemaAfter.id())) {
            schemaBefore = existedTableSchemas.get(schemaAfter.id()).getTable();
        }
        Optional<String> parent =
                sourceConfig.getPartitionRouting().parentOf(schemaDotTable(schemaAfter.id()));
        if (parent.isPresent()) {
            handleRoutedSchemaChange(toTableId(parent.get()), schemaBefore, schemaAfter, output);
            return;
        }
        maybeSendCreateTableEventFromCache(schemaAfter.id(), output);
        List<SchemaChangeEvent> schemaChangeEvents =
                inferSchemaChangeEvent(
                        schemaAfter.id(), schemaBefore, schemaAfter, sourceConfig, postgresDialect);
        LOG.info("Inferred Schema change events: {}", schemaChangeEvents);
        schemaChangeEvents.forEach(schemaChangeEvent -> output.collect((T) schemaChangeEvent));
    }

    /**
     * A child partition reports its own relation, but downstream only knows its partition root: a
     * new or swapped-in partition must not create a table of its own, and a column change applied
     * to the root is reported once by every partition, so it is emitted for the root only once.
     */
    private void handleRoutedSchemaChange(
            TableId parentId,
            @Nullable Table childBefore,
            Table childAfter,
            SourceOutput<T> output) {
        ensureCreateTableEventSent(parentId, childAfter.id(), output);
        if (childBefore == null) {
            return;
        }
        List<SchemaChangeEvent> events =
                evolveParentSchema(
                        parentSchemas,
                        parentId,
                        createTableEventCache.get(parentId).getSchema(),
                        inferSchemaChangeEvent(
                                parentId, childBefore, childAfter, sourceConfig, postgresDialect));
        LOG.info("Inferred schema change events of partition {}: {}", childAfter.id(), events);
        events.forEach(schemaChangeEvent -> output.collect((T) schemaChangeEvent));
    }

    /**
     * Applies the given events to the tracked schema of the partition root and returns those that
     * are not applied yet.
     */
    @VisibleForTesting
    static List<SchemaChangeEvent> evolveParentSchema(
            Map<TableId, Schema> parentSchemas,
            TableId parentId,
            Schema initialSchema,
            List<SchemaChangeEvent> events) {
        Schema schema = parentSchemas.getOrDefault(parentId, initialSchema);
        List<SchemaChangeEvent> evolving = new ArrayList<>();
        for (SchemaChangeEvent event : events) {
            if (!SchemaUtils.isSchemaChangeEventRedundant(schema, event)) {
                schema = SchemaUtils.applySchemaChangeEvent(schema, event);
                evolving.add(event);
            }
        }
        parentSchemas.put(parentId, schema);
        return evolving;
    }

    private void maybeSendCreateTableEventFromCache(TableId tableId, SourceOutput<T> output) {
        if (!alreadySendCreateTableTables.contains(tableId)) {
            CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
            if (createTableEvent != null) {
                sendCreateTableEvent(createTableEvent, output);
            }
            alreadySendCreateTableTables.add(tableId);
        }
    }

    private void sendCreateTableEvent(CreateTableEvent createTableEvent, SourceOutput<T> output) {
        output.collect((T) createTableEvent);
    }

    private CreateTableEvent getCreateTableEvent(TableId tableId, @Nullable TableId originChild) {
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Schema schema = resolveTableSchema(tableId, originChild, jdbc);
            return new CreateTableEvent(
                    toCdcTableId(
                            tableId,
                            sourceConfig.getDatabaseList().get(0),
                            includeDatabaseInTableId),
                    schema);
        }
    }

    /**
     * Returns the schema of the given table. A routed partition root takes its schema from one of
     * its partitions: on PostgreSQL 10 the root carries no primary key of its own, so it is never
     * looked up itself.
     */
    private Schema resolveTableSchema(
            TableId tableId, @Nullable TableId originChild, PostgresConnection jdbc) {
        PostgresPartitionRouting routing = sourceConfig.getPartitionRouting();
        if (routing.childrenOf(schemaDotTable(tableId)).isEmpty()) {
            return PostgresSchemaUtils.getTableSchema(tableId, sourceConfig, jdbc);
        }
        PostgresSchema dbzSchema = PostgresSchemaUtils.newDbzSchema(sourceConfig, jdbc);
        Table child =
                partitionOf(
                        tableId,
                        routing.sampleChildOf(schemaDotTable(tableId)),
                        originChild,
                        dbzSchema::tableFor);
        return PostgresSchemaUtils.toSchema(
                child, sourceConfig.getDbzConnectorConfig(), jdbc.getTypeRegistry());
    }

    /**
     * Returns the first visible of the sample partition and the partition the current event came
     * from.
     */
    @VisibleForTesting
    static Table partitionOf(
            TableId partitionRoot,
            Optional<String> sampleChild,
            @Nullable TableId originChild,
            Function<TableId, Table> lookup) {
        if (sampleChild.isPresent()) {
            Table table = lookup.apply(toTableId(sampleChild.get()));
            if (table != null) {
                return table;
            }
        }
        if (originChild != null && !originChild.equals(partitionRoot)) {
            Table table = lookup.apply(originChild);
            if (table != null) {
                return table;
            }
        }
        throw new IllegalStateException(
                String.format(
                        "Partition root %s: neither its sample partition %s nor partition %s is "
                                + "visible in the Debezium schema",
                        partitionRoot, sampleChild.orElse(null), originChild));
    }

    private TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Field field = source.schema().field(SCHEMA_NAME_KEY);
        String schemaName = null;
        if (field != null) {
            schemaName = source.getString(SCHEMA_NAME_KEY);
        }
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    /**
     * Rewrites a record of a child partition into a record of the configured partition root. Such a
     * server has no {@code publish_via_partition_root}, so the topic, the source metadata and the
     * table id of the event all report the identity of the partition and have to be corrected.
     */
    private SourceRecord routeToParentTable(SourceRecord element) {
        Struct value = (Struct) element.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null || source.schema().field(SCHEMA_NAME_KEY) == null) {
            return element;
        }
        Optional<String> parent =
                sourceConfig
                        .getPartitionRouting()
                        .parentOf(
                                source.getString(SCHEMA_NAME_KEY)
                                        + "."
                                        + source.getString(TABLE_NAME_KEY));
        if (!parent.isPresent()) {
            return element;
        }
        TableId parentTableId = toTableId(parent.get());
        source.put(SCHEMA_NAME_KEY, parentTableId.schema());
        source.put(TABLE_NAME_KEY, parentTableId.table());
        String topic = element.topic();
        return element.newRecord(
                topic.substring(0, topic.lastIndexOf('.') + 1) + parentTableId.table(),
                element.kafkaPartition(),
                element.keySchema(),
                element.key(),
                element.valueSchema(),
                element.value(),
                element.timestamp(),
                element.headers());
    }

    private static TableId toTableId(String schemaDotTable) {
        int separator = schemaDotTable.indexOf('.');
        return new TableId(
                null,
                schemaDotTable.substring(0, separator),
                schemaDotTable.substring(separator + 1));
    }

    private static String schemaDotTable(TableId tableId) {
        return tableId.schema() + "." + tableId.table();
    }

    private Map<TableId, CreateTableEvent> generateCreateTableEvent(
            PostgresSourceConfig sourceConfig) {
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Map<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
            List<TableId> capturedTableIds =
                    TableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            sourceConfig.includePartitionedTables());
            for (TableId tableId : capturedTableIds) {
                if (sourceConfig
                        .getPartitionRouting()
                        .parentOf(schemaDotTable(tableId))
                        .isPresent()) {
                    // The changes of a child partition are emitted as the configured parent table.
                    continue;
                }
                Schema schema = resolveTableSchema(tableId, null, jdbc);
                createTableEventCache.put(
                        tableId,
                        new CreateTableEvent(
                                toCdcTableId(
                                        tableId,
                                        this.sourceConfig.getDatabaseList().get(0),
                                        includeDatabaseInTableId),
                                schema));
            }
            return createTableEventCache;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
