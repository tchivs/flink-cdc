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
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetUtils;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.ChunkUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.DROP_SLOT_ON_STOP;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/** The context of {@link PostgresScanFetchTask} and {@link PostgresStreamFetchTask}. */
public class PostgresSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    static final String CONNECTION_NAME = "postgres-fetch-task-connection";

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceFetchTaskContext.class);

    private PostgresTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresConnection jdbcConnection;
    private ReplicationConnection replicationConnection;
    private PostgresOffsetContext offsetContext;
    private PostgresPartition partition;
    private RelationAwarePostgresSchema schema;
    private ErrorHandler errorHandler;
    private CDCPostgresDispatcher postgresDispatcher;
    private EventMetadataProvider metadataProvider;
    private SnapshotChangeEventSourceMetrics<PostgresPartition> snapshotChangeEventSourceMetrics;
    private Snapshotter snapShotter;
    private Map<TableId, TableId> childToParentMapping = Collections.emptyMap();
    private Map<TableId, List<TableId>> parentToChildrenMapping = Collections.emptyMap();
    private final Pg10FetchTaskContextCoordinator pg10Coordinator;

    public PostgresSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, PostgresDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.pg10Coordinator = new Pg10FetchTaskContextCoordinator(this);
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return (PostgresConnectorConfig) super.getDbzConnectorConfig();
    }

    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? new PostgresOffsetFactory()
                                .createInitialOffset() // get an offset for starting snapshot
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return PostgresOffsetUtils.getPostgresOffsetContext(loader, offset);
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        LOG.debug("Configuring PostgresSourceFetchTaskContext for split: {}", sourceSplitBase);
        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) sourceConfig;
        initializePartitionMappings(pgSourceConfig);

        PostgresConnectorConfig dbzConfig =
                createConfiguredConnectorConfig(sourceSplitBase, pgSourceConfig);
        setDbzConnectorConfig(dbzConfig);
        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        dbzConfig.getConfig().getString(SNAPSHOT_MODE));
        this.snapShotter = snapshotMode.getSnapshotter(dbzConfig.getConfig());

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        this.jdbcConnection =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(), valueConverterBuilder, CONNECTION_NAME);

        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);

        try {
            this.schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()),
                            sourceSplitBase.getTableSchemas().values());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresSchema", e);
        }

        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(dbzConfig), sourceSplitBase);
        this.partition = new PostgresPartition(dbzConfig.getLogicalName());
        this.taskContext = PostgresObjectUtils.newTaskContext(dbzConfig, schema, topicSelector);

        if (replicationConnection == null) {
            replicationConnection =
                    createReplicationConnection(
                            this.taskContext,
                            jdbcConnection,
                            this.snapShotter.shouldSnapshot(),
                            dbzConfig);
        }

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(dbzConfig.getPollInterval())
                        .maxBatchSize(dbzConfig.getMaxBatchSize())
                        .maxQueueSize(dbzConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(dbzConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "postgres-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();

        this.errorHandler = new PostgresErrorHandler(getDbzConnectorConfig(), queue);
        this.metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        PostgresConnectorConfig finalDbzConfig = dbzConfig;
        this.postgresDispatcher =
                new CDCPostgresDispatcher(
                        finalDbzConfig,
                        topicSelector,
                        schema,
                        queue,
                        finalDbzConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        new HeartbeatFactory<>(
                                dbzConfig,
                                topicSelector,
                                schemaNameAdjuster,
                                () ->
                                        new PostgresConnection(
                                                finalDbzConfig.getJdbcConfig(),
                                                PostgresConnection.CONNECTION_GENERAL),
                                exception -> {
                                    String sqlErrorId = exception.getSQLState();
                                    switch (sqlErrorId) {
                                        case "57P01":
                                            // Postgres error admin_shutdown, see
                                            // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                            throw new DebeziumException(
                                                    "Could not execute heartbeat action query (Error: "
                                                            + sqlErrorId
                                                            + ")",
                                                    exception);
                                        case "57P03":
                                            // Postgres error cannot_connect_now, see
                                            // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                            throw new RetriableException(
                                                    "Could not execute heartbeat action query (Error: "
                                                            + sqlErrorId
                                                            + ")",
                                                    exception);
                                        default:
                                            break;
                                    }
                                }),
                        schemaNameAdjuster,
                        childToParentMapping);
        schema.setDispatcher(postgresDispatcher);

        ChangeEventSourceMetricsFactory<PostgresPartition> metricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                metricsFactory.getSnapshotMetrics(taskContext, queue, metadataProvider);
    }

    @Override
    public PostgresSchema getDatabaseSchema() {
        return schema;
    }

    @Override
    public RowType getSplitType(Table table) {
        Column splitColumn = ChunkUtils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
        return ChunkUtils.getSplitType(splitColumn);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public PostgresEventDispatcher<TableId> getEventDispatcher() {
        return postgresDispatcher;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return postgresDispatcher;
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public PostgresPartition getPartition() {
        return partition;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        TableId tableId;
        if (record instanceof PostgresSchemaRecord) {
            tableId = ((PostgresSchemaRecord) record).getTable().id();
        } else {
            Struct value = (Struct) record.value();
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            String schemaName = source.getString(SCHEMA_NAME_KEY);
            String tableName = source.getString(TABLE_NAME_KEY);
            tableId = new TableId(null, schemaName, tableName);
        }
        return routeTableId(tableId);
    }

    public TableId routeTableId(TableId tableId) {
        if (tableId == null || childToParentMapping.isEmpty()) {
            return tableId;
        }
        return childToParentMapping.getOrDefault(tableId, tableId);
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return PostgresOffset.of(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (replicationConnection != null) {
            replicationConnection.close();
        }
    }

    public PostgresConnection getConnection() {
        return jdbcConnection;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    public ReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    public SnapshotChangeEventSourceMetrics<PostgresPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public Snapshotter getSnapShotter() {
        return snapShotter;
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        return PostgresConnectorConfig.LogicalDecoder.parse(
                        sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name()))
                .getPostgresPluginName();
    }

    private boolean isBackFillSplit(SourceSplitBase sourceSplitBase) {
        return sourceSplitBase.isStreamSplit()
                && !StreamSplit.STREAM_SPLIT_ID.equalsIgnoreCase(
                        sourceSplitBase.asStreamSplit().splitId());
    }

    private void initializePartitionMappings(PostgresSourceConfig pgSourceConfig) {
        this.childToParentMapping = pgSourceConfig.getChildToParentMappingOrEmpty();
        this.parentToChildrenMapping = pgSourceConfig.getParentToChildrenMappingOrEmpty();
    }

    private PostgresConnectorConfig createConfiguredConnectorConfig(
            SourceSplitBase sourceSplitBase, PostgresSourceConfig pgSourceConfig) {
        return createConfiguredConnectorConfig(
                sourceSplitBase, pgSourceConfig, childToParentMapping, parentToChildrenMapping);
    }

    PostgresConnectorConfig createConfiguredConnectorConfig(
            SourceSplitBase sourceSplitBase, Pg10CaptureState captureState) {
        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) sourceConfig;
        return createConfiguredConnectorConfig(
                sourceSplitBase,
                pgSourceConfig,
                captureState.getChildToParentMapping(),
                captureState.getParentToChildrenMapping());
    }

    private PostgresConnectorConfig createConfiguredConnectorConfig(
            SourceSplitBase sourceSplitBase,
            PostgresSourceConfig pgSourceConfig,
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        PostgresConnectorConfig dbzConfig = getDbzConnectorConfig();
        if (sourceSplitBase instanceof SnapshotSplit) {
            return buildSnapshotSplitConfig(
                    dbzConfig, (SnapshotSplit) sourceSplitBase, pgSourceConfig);
        }

        Configuration.Builder builder = dbzConfig.getConfig().edit();
        if (isBackFillSplit(sourceSplitBase)) {
            // when backfilled split, include both the parent table and its child partitions
            // to ensure WAL events from children are captured during incremental snapshot
            TableId backfillTableId =
                    sourceSplitBase.asStreamSplit().getTableSchemas().keySet().iterator().next();
            String includeList = getTableList(backfillTableId);
            if (!childToParentMapping.isEmpty()) {
                List<TableId> children =
                        getAllChildrenForParent(
                                backfillTableId, childToParentMapping, parentToChildrenMapping);
                if (!children.isEmpty()) {
                    includeList = appendChildTablesToIncludeList(includeList, children);
                }
            }
            builder.with("table.include.list", includeList);
        } else if (!childToParentMapping.isEmpty()) {
            String includeList = dbzConfig.getConfig().getString("table.include.list");
            builder.with(
                    "table.include.list",
                    appendChildTablesToIncludeList(includeList, childToParentMapping.keySet()));
        }

        return new PostgresConnectorConfig(
                builder
                        // never drop slot for stream split, which is also global split
                        .with(DROP_SLOT_ON_STOP.name(), false)
                        .build());
    }

    private PostgresConnectorConfig buildSnapshotSplitConfig(
            PostgresConnectorConfig dbzConfig,
            SnapshotSplit snapshotSplit,
            PostgresSourceConfig pgSourceConfig) {
        return new PostgresConnectorConfig(
                dbzConfig
                        .getConfig()
                        .edit()
                        .with("table.include.list", getTableList(snapshotSplit.getTableId()))
                        .with(SLOT_NAME.name(), pgSourceConfig.getSlotNameForBackfillTask())
                        // drop slot for backfill stream split
                        .with(DROP_SLOT_ON_STOP.name(), true)
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build());
    }

    private ChangeEventQueue<DataChangeEvent> createQueue(PostgresConnectorConfig dbzConfig) {
        return new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(dbzConfig.getPollInterval())
                .maxBatchSize(dbzConfig.getMaxBatchSize())
                .maxQueueSize(dbzConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(dbzConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(
                        () -> taskContext.configureLoggingContext("postgres-cdc-connector-task"))
                // do not buffer any element, we use signal event
                // .buffering()
                .build();
    }

    CDCPostgresDispatcher createDispatcher(
            PostgresConnectorConfig dbzConfig,
            TopicSelector<TableId> topicSelector,
            RelationAwarePostgresSchema schema,
            ChangeEventQueue<DataChangeEvent> queue,
            EventMetadataProvider metadataProvider) {
        return createDispatcher(
                dbzConfig, topicSelector, schema, queue, metadataProvider, childToParentMapping);
    }

    CDCPostgresDispatcher createDispatcher(
            PostgresConnectorConfig dbzConfig,
            TopicSelector<TableId> topicSelector,
            RelationAwarePostgresSchema schema,
            ChangeEventQueue<DataChangeEvent> queue,
            EventMetadataProvider metadataProvider,
            Map<TableId, TableId> childToParentMapping) {
        PostgresConnectorConfig finalDbzConfig = dbzConfig;
        return new CDCPostgresDispatcher(
                finalDbzConfig,
                topicSelector,
                schema,
                queue,
                finalDbzConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                new HeartbeatFactory<>(
                        dbzConfig,
                        topicSelector,
                        schemaNameAdjuster,
                        () ->
                                new PostgresConnection(
                                        finalDbzConfig.getJdbcConfig(),
                                        PostgresConnection.CONNECTION_GENERAL),
                        exception -> {
                            String sqlErrorId = exception.getSQLState();
                            switch (sqlErrorId) {
                                case "57P01":
                                    // Postgres error admin_shutdown, see
                                    // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                    throw new DebeziumException(
                                            "Could not execute heartbeat action query (Error: "
                                                    + sqlErrorId
                                                    + ")",
                                            exception);
                                case "57P03":
                                    // Postgres error cannot_connect_now, see
                                    // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                    throw new RetriableException(
                                            "Could not execute heartbeat action query (Error: "
                                                    + sqlErrorId
                                                    + ")",
                                            exception);
                                default:
                                    break;
                            }
                        }),
                schemaNameAdjuster,
                childToParentMapping);
    }

    private String getTableList(TableId tableId) {
        if (tableId.schema() == null || tableId.schema().isEmpty()) {
            return tableId.table();
        }
        return tableId.schema() + "." + tableId.table();
    }

    private String appendChildTablesToIncludeList(
            String includeList, Collection<TableId> childTables) {
        if (childTables.isEmpty()) {
            return includeList;
        }

        Set<String> includeEntries = new LinkedHashSet<>();
        if (includeList != null && !includeList.trim().isEmpty()) {
            String[] entries = includeList.split(",");
            for (String entry : entries) {
                String trimmed = entry.trim();
                if (!trimmed.isEmpty()) {
                    includeEntries.add(trimmed);
                }
            }
        }

        for (TableId childTable : childTables) {
            includeEntries.add(getTableList(childTable));
        }

        return String.join(",", includeEntries);
    }

    private List<TableId> getAllChildrenForParent(TableId parentTableId) {
        return getAllChildrenForParent(
                parentTableId, childToParentMapping, parentToChildrenMapping);
    }

    private List<TableId> getAllChildrenForParent(
            TableId parentTableId,
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        if (!parentToChildrenMapping.isEmpty()) {
            return new ArrayList<>(
                    parentToChildrenMapping.getOrDefault(parentTableId, Collections.emptyList()));
        }

        List<TableId> children = new ArrayList<>();
        for (Map.Entry<TableId, TableId> entry : childToParentMapping.entrySet()) {
            if (parentTableId.equals(entry.getValue())) {
                children.add(entry.getKey());
            }
        }
        return children;
    }

    public Pg10StreamingSessionRuntime buildStreamingRuntime(
            StreamSplit streamSplit,
            Pg10CaptureState captureState,
            PostgresOffsetContext startingOffsetContext) {
        return pg10Coordinator.buildStreamingRuntime(
                streamSplit, captureState, startingOffsetContext);
    }

    public void closeStreamingRuntime(Pg10StreamingSessionRuntime runtime) {
        pg10Coordinator.closeStreamingRuntime(runtime);
    }

    /** Builds the initial Pg10CaptureState from the current source configuration. */
    public Pg10CaptureState buildInitialCaptureState(SourceSplitBase split) {
        return pg10Coordinator.buildInitialCaptureState(split);
    }

    public PostgresOffsetContext getRestartOffsetContext() {
        return offsetContext;
    }

    public void setRestartOffsetContext(PostgresOffsetContext offsetContext) {
        this.offsetContext = offsetContext;
    }

    /**
     * Accepts a reconciled PG10 capture state for the next streaming session restart.
     *
     * <p>This acceptance boundary is where the runtime commits to the new routing mappings. The
     * restart offset handoff must happen at the same point so the follow-up streaming session is
     * rebuilt from the WAL position that was current when the new capture state was accepted.
     */
    public void acceptPg10CaptureStateForRestart(
            Pg10CaptureState captureState,
            PostgresOffsetContext restartOffsetContext,
            StreamSplit streamSplit) {
        pg10Coordinator.acceptPg10CaptureStateForRestart(
                captureState, restartOffsetContext, streamSplit);
    }

    public void syncPg10CaptureState(Pg10CaptureState captureState, StreamSplit streamSplit) {
        pg10Coordinator.syncPg10CaptureState(captureState, streamSplit);
    }

    /**
     * Reconciles current catalog state and returns the next accepted capture state.
     *
     * <p>Uses the provided JDBC connection to avoid opening a duplicate connection. The caller is
     * responsible for connection lifecycle management.
     */
    public java.util.Optional<Pg10CaptureState> maybePrepareNextCaptureState(
            io.debezium.jdbc.JdbcConnection jdbc,
            PostgresDialect postgresDialect,
            Pg10CaptureState currentCaptureState,
            List<TableId> parentTables,
            Set<TableId> publishedButUnacceptedChildren) {
        return pg10Coordinator.maybePrepareNextCaptureState(
                jdbc,
                postgresDialect,
                currentCaptureState,
                parentTables,
                publishedButUnacceptedChildren);
    }

    void setChildToParentMapping(Map<TableId, TableId> childToParentMapping) {
        this.childToParentMapping = childToParentMapping;
    }

    void setParentToChildrenMapping(Map<TableId, List<TableId>> parentToChildrenMapping) {
        this.parentToChildrenMapping = parentToChildrenMapping;
    }
}
