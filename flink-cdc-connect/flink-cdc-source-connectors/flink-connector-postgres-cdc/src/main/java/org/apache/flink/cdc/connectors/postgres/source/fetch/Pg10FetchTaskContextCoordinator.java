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

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionReconciler;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/** Coordinates PG10-specific capture-state and runtime operations for fetch tasks. */
final class Pg10FetchTaskContextCoordinator {

    private final PostgresSourceFetchTaskContext context;

    Pg10FetchTaskContextCoordinator(PostgresSourceFetchTaskContext context) {
        this.context = context;
    }

    Pg10StreamingSessionRuntime buildStreamingRuntime(
            StreamSplit streamSplit,
            Pg10CaptureState captureState,
            PostgresOffsetContext startingOffsetContext) {
        PostgresConnectorConfig dbzConfig =
                context.createConfiguredConnectorConfig(streamSplit, captureState);

        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        dbzConfig.getConfig().getString(SNAPSHOT_MODE));
        Snapshotter snapShotter = snapshotMode.getSnapshotter(dbzConfig.getConfig());

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbcConnection =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        PostgresSourceFetchTaskContext.CONNECTION_NAME);

        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);

        RelationAwarePostgresSchema schema;
        try {
            schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresSchema", e);
        }

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());
        PostgresTaskContext taskContext =
                PostgresObjectUtils.newTaskContext(dbzConfig, schema, topicSelector);

        ReplicationConnection replicationConnection =
                createReplicationConnection(
                        taskContext, jdbcConnection, snapShotter.shouldSnapshot(), dbzConfig);

        ErrorHandler errorHandler = new PostgresErrorHandler(dbzConfig, context.getQueue());
        EventMetadataProvider metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        CDCPostgresDispatcher dispatcher =
                context.createDispatcher(
                        dbzConfig,
                        topicSelector,
                        schema,
                        context.getQueue(),
                        metadataProvider,
                        captureState.getChildToParentMapping());
        schema.setDispatcher(dispatcher);

        return new Pg10StreamingSessionRuntime(
                dbzConfig,
                jdbcConnection,
                replicationConnection,
                schema,
                dispatcher,
                errorHandler,
                taskContext,
                partition,
                snapShotter,
                startingOffsetContext,
                context.getQueue(),
                captureState);
    }

    void closeStreamingRuntime(Pg10StreamingSessionRuntime runtime) {
        runtime.close();
    }

    Pg10CaptureState buildInitialCaptureState(SourceSplitBase split) {
        if (split.isStreamSplit()) {
            StreamSplit streamSplit = split.asStreamSplit();
            if (streamSplit.isPg10RoutingStateInitialized()) {
                Pg10CaptureState restoredCaptureState =
                        Pg10CaptureState.of(
                                streamSplit.getPg10ChildToParentMapping(),
                                streamSplit.getPg10ParentToChildrenMapping());
                validateRestoredCaptureState(streamSplit, restoredCaptureState);
                syncPg10CaptureState(restoredCaptureState, streamSplit);
                return restoredCaptureState;
            }
        }

        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) context.getSourceConfig();
        return Pg10CaptureState.of(
                pgSourceConfig.getChildToParentMappingOrEmpty(),
                pgSourceConfig.getParentToChildrenMappingOrEmpty());
    }

    private void validateRestoredCaptureState(
            StreamSplit streamSplit, Pg10CaptureState restoredCaptureState) {
        if (!(context.getDataSourceDialect() instanceof PostgresDialect)
                || context.getConnection() == null) {
            return;
        }

        List<TableId> parentTables =
                derivePg10ParentTables(
                        streamSplit, restoredCaptureState, context.getSourceConfig());
        if (parentTables.isEmpty()) {
            parentTables.addAll(restoredCaptureState.getParentToChildrenMapping().keySet());
        }

        try {
            ((PostgresDialect) context.getDataSourceDialect())
                    .validateRestoredPg10CaptureState(
                            context.getConnection(), restoredCaptureState, parentTables);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Failed to validate restored PG10 capture state: parentTables=" + parentTables,
                    e);
        }
    }

    void acceptPg10CaptureStateForRestart(
            Pg10CaptureState captureState,
            PostgresOffsetContext restartOffsetContext,
            StreamSplit streamSplit) {
        context.setRestartOffsetContext(
                clampRestartOffsetToCommittedBoundary(restartOffsetContext));
        syncPg10CaptureState(captureState, streamSplit);
    }

    private PostgresOffsetContext clampRestartOffsetToCommittedBoundary(
            PostgresOffsetContext restartOffsetContext) {
        if (restartOffsetContext == null) {
            return null;
        }

        Map<String, ?> restartOffset = restartOffsetContext.getOffset();
        Long lastCommitLsn = readLong(restartOffset, PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        if (lastCommitLsn == null) {
            return restartOffsetContext;
        }

        Long currentLsn = readLong(restartOffset, SourceInfo.LSN_KEY);
        Long lastCompletelyProcessedLsn =
                readLong(restartOffset, PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        if ((currentLsn == null || currentLsn <= lastCommitLsn)
                && (lastCompletelyProcessedLsn == null
                        || lastCompletelyProcessedLsn <= lastCommitLsn)) {
            return restartOffsetContext;
        }

        Map<String, Object> sanitizedOffset = new HashMap<>(restartOffset);
        sanitizedOffset.put(SourceInfo.LSN_KEY, lastCommitLsn);
        sanitizedOffset.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCommitLsn);
        return new PostgresOffsetContext.Loader(context.getDbzConnectorConfig())
                .load(sanitizedOffset);
    }

    private Long readLong(Map<String, ?> offset, String key) {
        Object value = offset.get(key);
        return value == null ? null : ((Number) value).longValue();
    }

    static List<TableId> derivePg10ParentTables(
            StreamSplit streamSplit,
            Pg10CaptureState captureState,
            org.apache.flink.cdc.connectors.base.config.SourceConfig sourceConfig) {
        LinkedHashMap<TableId, TableId> knownChildToParent = new LinkedHashMap<>();
        if (sourceConfig instanceof PostgresSourceConfig) {
            knownChildToParent.putAll(
                    ((PostgresSourceConfig) sourceConfig).getChildToParentMappingOrEmpty());
        }
        if (captureState != null) {
            knownChildToParent.putAll(captureState.getChildToParentMapping());
        }

        List<TableId> parentTables = new ArrayList<>();
        for (TableId tableId : streamSplit.getTableSchemas().keySet()) {
            if (!knownChildToParent.containsKey(tableId)) {
                parentTables.add(tableId);
            }
        }

        if (parentTables.isEmpty() && captureState != null) {
            parentTables.addAll(captureState.getParentToChildrenMapping().keySet());
        }
        return parentTables;
    }

    void syncPg10CaptureState(Pg10CaptureState captureState, StreamSplit streamSplit) {
        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) context.getSourceConfig();
        pgSourceConfig.setChildToParentMapping(captureState.getChildToParentMapping());
        pgSourceConfig.setParentToChildrenMapping(captureState.getParentToChildrenMapping());
        pgSourceConfig.setPg10PartitionMappingInitialized(true);

        context.setChildToParentMapping(captureState.getChildToParentMapping());
        context.setParentToChildrenMapping(captureState.getParentToChildrenMapping());

        PostgresConnectorConfig dbzConfig =
                context.createConfiguredConnectorConfig(streamSplit, captureState);
        context.setDbzConnectorConfig(dbzConfig);
    }

    void cachePg10CompensationTableSchemas(Map<TableId, TableChanges.TableChange> tableSchemas) {
        if (tableSchemas.isEmpty()) {
            return;
        }

        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) context.getSourceConfig();
        Map<TableId, TableChanges.TableChange> mergedSchemas =
                new LinkedHashMap<>(pgSourceConfig.getPg10CompensationTableSchemasOrEmpty());
        mergedSchemas.putAll(tableSchemas);
        pgSourceConfig.setPg10CompensationTableSchemas(mergedSchemas);
        context.setCompensationTableSchemas(mergedSchemas);
    }

    Optional<Pg10CaptureState> maybePrepareNextCaptureState(
            io.debezium.jdbc.JdbcConnection jdbc,
            PostgresDialect postgresDialect,
            Pg10CaptureState currentCaptureState,
            List<TableId> parentTables,
            Set<TableId> publishedButUnacceptedChildren) {
        try {
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                    postgresDialect.reconcilePg10PartitionMappings(
                            jdbc, currentCaptureState, parentTables);
            Pg10CaptureState nextCaptureState =
                    buildNextAcceptedCaptureState(
                            currentCaptureState, reconcileResult, publishedButUnacceptedChildren);
            if (hasSameMappings(currentCaptureState, nextCaptureState)) {
                return Optional.empty();
            }
            return Optional.of(nextCaptureState);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Failed to prepare next PG10 capture state: parentTables=" + parentTables, e);
        }
    }

    private Pg10CaptureState buildNextAcceptedCaptureState(
            Pg10CaptureState currentCaptureState,
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult,
            Set<TableId> publishedButUnacceptedChildren) {
        Map<TableId, TableId> currentAcceptedChildToParent =
                currentCaptureState.getChildToParentMapping();
        Map<TableId, TableId> latestChildToParent = reconcileResult.getLatestChildToParent();
        Map<TableId, TableId> nextAcceptedChildToParent = new LinkedHashMap<>();
        for (Map.Entry<TableId, TableId> entry : latestChildToParent.entrySet()) {
            TableId childTable = entry.getKey();
            if (currentAcceptedChildToParent.containsKey(childTable)
                    || publishedButUnacceptedChildren.contains(childTable)) {
                nextAcceptedChildToParent.put(childTable, entry.getValue());
            }
        }

        Map<TableId, List<TableId>> nextAcceptedParentToChildren = new LinkedHashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry :
                reconcileResult.getLatestParentToChildren().entrySet()) {
            List<TableId> acceptedChildren = new ArrayList<>();
            for (TableId childTable : entry.getValue()) {
                if (nextAcceptedChildToParent.containsKey(childTable)) {
                    acceptedChildren.add(childTable);
                }
            }
            if (!acceptedChildren.isEmpty()) {
                nextAcceptedParentToChildren.put(entry.getKey(), acceptedChildren);
            }
        }

        return Pg10CaptureState.of(nextAcceptedChildToParent, nextAcceptedParentToChildren);
    }

    private boolean hasSameMappings(
            Pg10CaptureState currentCaptureState, Pg10CaptureState nextCaptureState) {
        return currentCaptureState
                        .getChildToParentMapping()
                        .equals(nextCaptureState.getChildToParentMapping())
                && currentCaptureState
                        .getParentToChildrenMapping()
                        .equals(nextCaptureState.getParentToChildrenMapping());
    }
}
