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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final Set<TableId> partitionRouteMisses;

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
        this.partitionRouteMisses = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
        this.createTableEventCache.putAll(generateCreateTableEvent(sourceConfig));
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

                Table table = routePartitionTable(tableChange.getTable());
                TableId routedTableId = table.id();
                CreateTableEvent createTableEvent =
                        toCreateTableEvent(table, sourceConfig, postgresDialect);
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .applyChangeEvent(createTableEvent);
                // Also populate createTableEventCache so handleDataChangeRecord
                // can find it without calling getCreateTableEvent. The downstream
                // schema lookup (DebeziumEventDeserializationSchema.getCreateTableEvent)
                // would otherwise query the JDBC connection for a PARTITIONED TABLE
                // by name and return null: PostgreSQL stores PARTITIONED TABLEs as
                // logical parents and only child partitions appear in
                // information_schema.tables. Populating the cache here keeps the
                // DML path consistent with the schema path and avoids the
                // "CreateTableEvent not found" failure mode reported in #3837.
                // putIfAbsent preserves the first-seen entry in case the same
                // tableId appears across multiple splits (e.g. after a restart
                // replays the same StreamSplit's TableSchemas).
                // This cache is owned by the reader/emitter pipeline and is accessed from
                // Flink's single-threaded source reader path. If that ownership changes, replace
                // the underlying DebeziumEventDeserializationSchema cache with a concurrent map.
                createTableEventCache.putIfAbsent(routedTableId, createTableEvent);
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
            handleDataChangeRecord(element, output);
        } else if (isSchemaChangeEvent(element) && element instanceof PostgresSchemaRecord) {
            PostgresSchemaRecord schemaRecord = (PostgresSchemaRecord) element;
            if (sourceConfig.isIncludeSchemaChanges()) {
                handleSchemaChangeRecord(schemaRecord, output, splitState);
            }
            recordRoutedSchema(schemaRecord, splitState);
            return;
        } else if (isSchemaChangeEvent(element) && sourceConfig.isIncludeSchemaChanges()) {
            handleSchemaChangeRecord(element, output, splitState);
        }
        super.processElement(element, output, splitState);
    }

    private void handleDataChangeRecord(SourceRecord element, SourceOutput<T> output) {
        // Source struct already carries the routed parent table info (set by
        // CDCPostgresDispatcher.preRouteTableId + offsetContext.updateWalPosition)
        TableId tableId = routePartitionTableId(getTableId(element));
        maybeSendCreateTableEventFromCache(tableId, output);
        // In rare case, we may miss some CreateTableEvents before DataChangeEvents.
        // Don't send CreateTableEvent for SchemaChangeEvents as it's the latest schema.
        if (!createTableEventCache.containsKey(tableId)) {
            CreateTableEvent createTableEvent = getCreateTableEvent(sourceConfig, tableId);
            sendCreateTableEvent(createTableEvent, output);
            createTableEventCache.put(tableId, createTableEvent);
            alreadySendCreateTableTables.add(tableId);
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
        Table originalSchemaAfter = schemaRecord.getTable();
        Table schemaAfter = routePartitionTable(originalSchemaAfter);
        maybeSendCreateTableEventFromCache(schemaAfter.id(), output);
        Table schemaBefore = null;
        TableChanges.TableChange schemaBeforeChange = existedTableSchemas.get(schemaAfter.id());
        if (schemaBeforeChange == null && !schemaAfter.id().equals(originalSchemaAfter.id())) {
            schemaBeforeChange = existedTableSchemas.get(originalSchemaAfter.id());
        }
        if (schemaBeforeChange != null) {
            schemaBefore = routePartitionTable(schemaBeforeChange.getTable());
        }
        List<SchemaChangeEvent> schemaChangeEvents =
                inferSchemaChangeEvent(
                        schemaAfter.id(), schemaBefore, schemaAfter, sourceConfig, postgresDialect);
        LOG.info("Inferred Schema change events: {}", schemaChangeEvents);
        schemaChangeEvents.forEach(schemaChangeEvent -> output.collect((T) schemaChangeEvent));
    }

    private void recordRoutedSchema(
            PostgresSchemaRecord schemaRecord, SourceSplitState splitState) {
        if (!splitState.isStreamSplitState()) {
            return;
        }
        Table routedTable = routePartitionTable(schemaRecord.getTable());
        splitState
                .asStreamSplitState()
                .recordSchema(
                        routedTable.id(),
                        new TableChanges.TableChange(
                                TableChanges.TableChangeType.CREATE, routedTable));
    }

    private void maybeSendCreateTableEventFromCache(TableId tableId, SourceOutput<T> output) {
        TableId routedTableId = routePartitionTableId(tableId);
        if (!alreadySendCreateTableTables.contains(routedTableId)) {
            CreateTableEvent createTableEvent = createTableEventCache.get(routedTableId);
            if (createTableEvent != null) {
                sendCreateTableEvent(createTableEvent, output);
                alreadySendCreateTableTables.add(routedTableId);
            }
            // Do NOT mark as sent on cache miss: let subsequent data change or schema
            // change paths fall back to JDBC-based CreateTableEvent generation.
        }
    }

    private void sendCreateTableEvent(CreateTableEvent createTableEvent, SourceOutput<T> output) {
        output.collect((T) createTableEvent);
    }

    private CreateTableEvent getCreateTableEvent(
            PostgresSourceConfig sourceConfig, TableId tableId) {
        TableId routedTableId = routePartitionTableId(tableId);
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Schema schema = PostgresSchemaUtils.getTableSchema(routedTableId, sourceConfig, jdbc);
            return new CreateTableEvent(
                    toCdcTableId(
                            routedTableId,
                            sourceConfig.getDatabaseList().get(0),
                            includeDatabaseInTableId),
                    schema);
        }
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

    private Table routePartitionTable(Table table) {
        TableId routedTableId = routePartitionTableId(table.id());
        if (routedTableId.equals(table.id())) {
            return table;
        }
        return table.edit().tableId(routedTableId).create();
    }

    private TableId routePartitionTableId(TableId tableId) {
        TableId routedTableId = postgresDialect.getChildToParentMapping().get(tableId);
        if (routedTableId != null) {
            return routedTableId;
        }
        if (!sourceConfig.includePartitionedTables()
                || sourceConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)
                || partitionRouteMisses.contains(tableId)) {
            return tableId;
        }

        postgresDialect.discoverPartitionState(Collections.singletonList(tableId));
        routedTableId = postgresDialect.getChildToParentMapping().get(tableId);
        if (routedTableId == null) {
            partitionRouteMisses.add(tableId);
            return tableId;
        }
        return routedTableId;
    }

    private Map<TableId, CreateTableEvent> generateCreateTableEvent(
            PostgresSourceConfig sourceConfig) {
        Map<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
        Map<TableId, TableChanges.TableChange> tableSchemas =
                postgresDialect.discoverDataCollectionSchemas(sourceConfig);
        for (TableChanges.TableChange tableSchema : tableSchemas.values()) {
            Table table = routePartitionTable(tableSchema.getTable());
            createTableEventCache.put(
                    table.id(), toCreateTableEvent(table, sourceConfig, postgresDialect));
        }
        return createTableEventCache;
    }
}
