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

import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionReconciler;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PublicationManager;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isEndWatermarkEvent;

/**
 * PG10-specific streaming fetch task that supports dynamic child partition discovery and session
 * restart.
 *
 * <p>When PostgreSQL 10 partitioned tables are enabled, new child partitions can be created at
 * runtime. This subclass extends the base streaming task with:
 *
 * <ul>
 *   <li>A <b>session restart loop</b> that rebuilds Debezium runtime objects when new partitions
 *       are discovered.
 *   <li>A <b>Relation message listener</b> that detects new child partitions in the WAL stream with
 *       zero latency, leveraging the pgoutput protocol guarantee that a Relation ('R') message is
 *       sent before any DML for a new table.
 *   <li>A <b>lightweight publication poller</b> with configurable steady/startup intervals that
 *       adds newly created child partitions to the publication via ALTER PUBLICATION ADD TABLE — a
 *       prerequisite for pgoutput to send Relation messages for those tables.
 * </ul>
 *
 * <p>For non-PG10 scenarios, use the base {@link PostgresStreamFetchTask} directly.
 */
public class Pg10StreamFetchTask extends PostgresStreamFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(Pg10StreamFetchTask.class);
    private static final String PG10_CHILD_DISCOVERED_LOG =
            "PG10 child discovered [splitId={}, publication={}, children={}, childCount={}, acceptedChildCount={}].";
    private static final String PG10_RELATION_RESTART_REQUESTED_LOG =
            "PG10 relation-triggered restart requested [splitId={}, child={}, publishedButUnacceptedChildren={}].";
    private static final String PG10_POLL_RESTART_REQUESTED_LOG =
            "PG10 poll-triggered restart requested [splitId={}, publication={}, child={}, confirmedChildren={}, childCount={}].";
    private static final String PG10_COMPENSATION_BACKFILL_STARTED_LOG =
            "PG10 compensation backfill started [splitId={}, child={}, restartBoundary={}].";
    private static final String PG10_COMPENSATION_BACKFILL_COMPLETED_LOG =
            "PG10 compensation backfill completed [splitId={}, child={}, forwardedRecordCount={}, restartBoundary={}].";
    private static final String PG10_COMPENSATION_BACKFILL_SKIPPED_LOG =
            "PG10 compensation backfill skipped [splitId={}, acceptedChildren={}, childCount={}, reason={}].";

    private volatile Pg10CaptureState currentCaptureState;
    private volatile PostgresOffsetContext currentOffsetContext;
    private volatile Set<TableId> publishedButUnacceptedChildren = Collections.emptySet();

    /** Tracked so that close() can interrupt it across session restarts. */
    private volatile Thread activePublicationPoller;

    public Pg10StreamFetchTask(StreamSplit streamSplit) {
        super(streamSplit);
    }

    @Override
    public void close() {
        Thread poller = activePublicationPoller;
        if (poller != null) {
            poller.interrupt();
        }
        publishedButUnacceptedChildren = Collections.emptySet();
        super.close();
    }

    @Override
    public void execute(Context context) throws Exception {
        if (isStopped()) {
            LOG.debug(
                    "Pg10StreamFetchTask for split: {} is already stopped and can not be executed",
                    getSplit());
            return;
        } else {
            LOG.debug("execute Pg10StreamFetchTask for split: {}", getSplit());
        }

        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        setTaskRunning(true);
        try {
            boolean restartRequired;
            publishedButUnacceptedChildren = Collections.emptySet();
            currentCaptureState = sourceFetchContext.buildInitialCaptureState(getSplit());
            currentOffsetContext = sourceFetchContext.getRestartOffsetContext();
            do {
                if (currentCaptureState == null || currentOffsetContext == null) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "PG10 streaming session state is not initialized for split %s",
                                    getSplit()));
                }

                Pg10StreamingSessionRuntime runtime =
                        sourceFetchContext.buildStreamingRuntime(
                                getSplit().asStreamSplit(),
                                currentCaptureState,
                                currentOffsetContext);

                try {
                    StoppableChangeEventSourceContext sessionContext =
                            new StoppableChangeEventSourceContext();
                    setChangeEventSourceContext(sessionContext);
                    restartRequired =
                            runStreamingSession(
                                    sourceFetchContext,
                                    runtime,
                                    currentCaptureState,
                                    sessionContext);
                } finally {
                    sourceFetchContext.closeStreamingRuntime(runtime);
                }
            } while (restartRequired && !isStopped());
        } finally {
            setTaskRunning(false);
        }
    }

    private boolean runStreamingSession(
            PostgresSourceFetchTaskContext sourceFetchContext,
            Pg10StreamingSessionRuntime runtime,
            Pg10CaptureState captureState,
            StoppableChangeEventSourceContext sessionContext)
            throws Exception {
        AtomicBoolean refreshRequested = new AtomicBoolean(false);
        AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
        AtomicReference<TableId> relationRestartTrigger = new AtomicReference<>();
        AtomicReference<String> restartTriggerSource = new AtomicReference<>();
        AtomicReference<PostgresOffsetContext> frozenRestartOffset = new AtomicReference<>();

        // Register a Relation message listener on the schema so that when pgoutput
        // sends a Relation ('R') message for a published-but-unaccepted child partition,
        // we immediately trigger a session restart instead of polling.
        installPg10RelationListener(
                runtime.getSchema(),
                captureState,
                runtime.getOffsetContext(),
                sourceFetchContext.getDbzConnectorConfig(),
                sessionContext,
                refreshRequested,
                relationRestartTrigger,
                restartTriggerSource,
                frozenRestartOffset,
                runtime.getReplicationConnection());

        // Start a lightweight publication poller that owns publication mutation and can trigger
        // a fallback restart once newly discovered children are confirmed visible in the
        // publication. This covers the case where the old blocked streaming session would
        // otherwise miss the first child DML before a Relation-driven restart can happen.
        Thread publicationPoller =
                startPg10PublicationPoller(
                        sourceFetchContext,
                        sessionContext,
                        refreshFailure,
                        refreshRequested,
                        relationRestartTrigger,
                        restartTriggerSource,
                        frozenRestartOffset,
                        runtime.getOffsetContext(),
                        runtime.getReplicationConnection());

        StreamSplitReadTask readTask =
                createStreamSplitReadTask(
                        runtime.getDbzConnectorConfig(),
                        runtime.getSnapShotter(),
                        runtime.getJdbcConnection(),
                        runtime.getDispatcher(),
                        runtime.getDispatcher(),
                        runtime.getErrorHandler(),
                        runtime.getTaskContext().getClock(),
                        runtime.getSchema(),
                        runtime.getTaskContext(),
                        runtime.getReplicationConnection(),
                        getSplit().asStreamSplit());
        setStreamSplitReadTask(readTask);

        readTask.execute(sessionContext, runtime.getPartition(), runtime.getOffsetContext());

        // Clean up: remove the listener and wait for the publication poller to finish
        runtime.getSchema().setPartitionListener(null);
        activePublicationPoller = null;
        if (publicationPoller != null) {
            publicationPoller.interrupt();
            publicationPoller.join();
        }
        if (refreshFailure.get() != null) {
            // Avoid double-wrapping: if the cause is already a FlinkRuntimeException, rethrow it
            Throwable cause = refreshFailure.get();
            if (cause instanceof FlinkRuntimeException) {
                throw (FlinkRuntimeException) cause;
            }
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed during PG10 publication polling for split %s", getSplit()),
                    cause);
        }

        if (!refreshRequested.get()) {
            return false;
        }

        // A relation- or poller-triggered restart was requested. Reconcile the new state.
        PostgresDialect postgresDialect =
                (PostgresDialect) sourceFetchContext.getDataSourceDialect();
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        List<TableId> parentTables =
                Pg10FetchTaskContextCoordinator.derivePg10ParentTables(
                        getSplit().asStreamSplit(),
                        captureState,
                        sourceFetchContext.getSourceConfig());

        // Use a single JDBC connection for reconciliation. Publication mutation remains
        // poller-owned.
        try (PostgresConnection jdbc = postgresDialect.openJdbcConnection()) {
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                    postgresDialect.reconcilePg10PartitionMappings(
                            jdbc, captureState, parentTables);

            List<TableId> newChildren = reconcileResult.getNewChildren();
            if (!newChildren.isEmpty()) {
                PostgresOffsetContext restartOffsetContext =
                        Optional.ofNullable(frozenRestartOffset.get())
                                .orElseGet(
                                        () ->
                                                freezeRestartOffsetAtLastCommit(
                                                        runtime.getOffsetContext(),
                                                        pgSourceConfig.getDbzConnectorConfig()));
                Optional<Pg10CaptureState> nextCaptureState =
                        sourceFetchContext.maybePrepareNextCaptureState(
                                jdbc,
                                postgresDialect,
                                captureState,
                                parentTables,
                                publishedButUnacceptedChildren);
                if (nextCaptureState.isPresent()) {
                    Pg10CaptureState acceptedCaptureState = nextCaptureState.get();
                    List<TableId> acceptedNewChildren =
                            getAcceptedNewChildren(
                                    captureState, acceptedCaptureState, reconcileResult);
                    maybePreparePg10CompensationBackfill(
                            sourceFetchContext,
                            pgSourceConfig,
                            acceptedNewChildren,
                            restartOffsetContext);
                    this.currentCaptureState = acceptedCaptureState;
                    removeAcceptedChildrenFromRestartGate(
                            this.currentCaptureState.getChildToParentMapping());
                    this.currentOffsetContext = restartOffsetContext;
                    sourceFetchContext.acceptPg10CaptureStateForRestart(
                            currentCaptureState, currentOffsetContext, getSplit().asStreamSplit());
                    LOG.info(
                            "PG10 accepted runtime child partitions [split={}, trigger={}, acceptedChildren={}, acceptedChildCount={}].",
                            getSplit().splitId(),
                            Optional.ofNullable(restartTriggerSource.get()).orElse("unknown"),
                            acceptedNewChildren,
                            acceptedNewChildren.size());
                    LOG.info(
                            "Restarting PG10 streaming session after {}-triggered detection "
                                    + "of new child partitions: {}",
                            Optional.ofNullable(restartTriggerSource.get()).orElse("unknown"),
                            newChildren);
                    return true;
                }
            }

            TableId restartTriggerTable = relationRestartTrigger.get();
            String triggerSource = restartTriggerSource.get();
            LOG.info(
                    "PG10 {}-triggered restart for target '{}' did not reconcile to a new "
                            + "accepted child partition from the current published-but-"
                            + "unaccepted restart gate; keeping the existing capture-state baseline.",
                    triggerSource == null ? "unknown" : triggerSource,
                    restartTriggerTable);
        }

        return false;
    }

    private void maybePreparePg10CompensationBackfill(
            PostgresSourceFetchTaskContext sourceFetchContext,
            PostgresSourceConfig pgSourceConfig,
            List<TableId> acceptedNewChildren,
            PostgresOffsetContext restartOffsetContext) {
        if (acceptedNewChildren.isEmpty()) {
            LOG.debug(
                    PG10_COMPENSATION_BACKFILL_SKIPPED_LOG,
                    getSplit().splitId(),
                    acceptedNewChildren,
                    acceptedNewChildren.size(),
                    "no-accepted-runtime-children");
            return;
        }

        if (!pgSourceConfig.isPg10ChildPartitionBackfillEnabled()) {
            LOG.info(
                    PG10_COMPENSATION_BACKFILL_SKIPPED_LOG,
                    getSplit().splitId(),
                    acceptedNewChildren,
                    acceptedNewChildren.size(),
                    "disabled");
            return;
        }

        Map<TableId, io.debezium.relational.history.TableChanges.TableChange> compensationSchemas =
                sourceFetchContext.discoverCompensationTableSchemas(acceptedNewChildren);
        sourceFetchContext.cachePg10CompensationTableSchemas(compensationSchemas);
        PostgresOffset restartBoundary = PostgresOffset.of(restartOffsetContext.getOffset());

        List<FinishedSnapshotSplitInfo> compensationFinishedSplitInfos = new ArrayList<>();
        for (TableId childTableId : acceptedNewChildren) {
            SnapshotSplit compensationSnapshotSplit =
                    sourceFetchContext.createPg10CompensationSnapshotSplit(childTableId);
            LOG.info(
                    PG10_COMPENSATION_BACKFILL_STARTED_LOG,
                    getSplit().splitId(),
                    childTableId,
                    restartBoundary);
            long forwardedRecordCount =
                    emitPg10CompensationSnapshot(
                            sourceFetchContext, compensationSnapshotSplit, restartOffsetContext);
            SnapshotSplit finishedCompensationSplit =
                    markPg10CompensationSplitFinished(compensationSnapshotSplit, restartBoundary);
            compensationFinishedSplitInfos.add(
                    sourceFetchContext.createPg10CompensationFinishedSplitInfo(
                            finishedCompensationSplit));
            LOG.info(
                    PG10_COMPENSATION_BACKFILL_COMPLETED_LOG,
                    getSplit().splitId(),
                    childTableId,
                    forwardedRecordCount,
                    restartBoundary);
        }
        sourceFetchContext.cachePg10CompensationFinishedSplitInfos(compensationFinishedSplitInfos);
        LOG.info(
                "Prepared bounded PG10 compensating backfill metadata for accepted runtime child partitions {} anchored to accepted restart boundary {}. "
                        + "This bounded compensation helps close the publication gap for newly accepted children, but it is not a WAL-perfect reconstruction of publication-invisible history or a stronger restart-boundary guarantee than implemented.",
                acceptedNewChildren,
                restartBoundary);
    }

    private long emitPg10CompensationSnapshot(
            PostgresSourceFetchTaskContext sourceFetchContext,
            SnapshotSplit compensationSnapshotSplit,
            PostgresOffsetContext restartOffsetContext) {
        PostgresSourceFetchTaskContext compensationContext =
                sourceFetchContext.createPg10CompensationFetchContext();
        PostgresScanFetchTask compensationFetchTask =
                new PostgresScanFetchTask(compensationSnapshotSplit);
        try {
            compensationContext.configure(compensationSnapshotSplit);
            compensationContext.setRestartOffsetContext(restartOffsetContext);
            compensationFetchTask.execute(compensationContext);
            return drainPg10CompensationQueue(
                    sourceFetchContext, compensationContext, compensationFetchTask);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to execute PG10 bounded compensating backfill for child "
                                    + "partition '%s'",
                            compensationSnapshotSplit.getTableId()),
                    e);
        } finally {
            try {
                compensationContext.close();
            } catch (Exception e) {
                LOG.debug(
                        "Ignored exception while closing PG10 compensation fetch context for '{}'",
                        compensationSnapshotSplit.getTableId(),
                        e);
            }
        }
    }

    private long drainPg10CompensationQueue(
            PostgresSourceFetchTaskContext sourceFetchContext,
            PostgresSourceFetchTaskContext compensationContext,
            PostgresScanFetchTask compensationFetchTask)
            throws InterruptedException {
        boolean reachedEnd = false;
        long forwardedRecordCount = 0L;
        while (!reachedEnd) {
            List<DataChangeEvent> batch = compensationContext.getQueue().poll();
            for (DataChangeEvent event : batch) {
                SourceRecord record = event.getRecord();
                if (isEndWatermarkEvent(record)) {
                    reachedEnd = true;
                    continue;
                }
                sourceFetchContext.getQueue().enqueue(new DataChangeEvent(record));
                forwardedRecordCount++;
            }

            if (!reachedEnd && !compensationFetchTask.isRunning() && batch.isEmpty()) {
                throw new FlinkRuntimeException(
                        String.format(
                                "PG10 compensation snapshot '%s' finished without end watermark event",
                                compensationFetchTask.getSplit()));
            }
        }
        return forwardedRecordCount;
    }

    private List<TableId> getAcceptedNewChildren(
            Pg10CaptureState previousCaptureState,
            Pg10CaptureState acceptedCaptureState,
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult) {
        LinkedHashSet<TableId> acceptedChildren =
                new LinkedHashSet<>(acceptedCaptureState.getChildToParentMapping().keySet());
        acceptedChildren.removeAll(previousCaptureState.getChildToParentMapping().keySet());
        if (acceptedChildren.isEmpty()) {
            return Collections.emptyList();
        }

        List<TableId> orderedAcceptedChildren = new ArrayList<>();
        for (TableId childTableId : reconcileResult.getNewChildren()) {
            if (acceptedChildren.contains(childTableId)) {
                orderedAcceptedChildren.add(childTableId);
            }
        }
        return orderedAcceptedChildren;
    }

    private SnapshotSplit markPg10CompensationSplitFinished(
            SnapshotSplit snapshotSplit,
            org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset highWatermark) {
        return new SnapshotSplit(
                snapshotSplit.getTableId(),
                snapshotSplit.splitId(),
                snapshotSplit.getSplitKeyType(),
                snapshotSplit.getSplitStart(),
                snapshotSplit.getSplitEnd(),
                highWatermark,
                snapshotSplit.getTableSchemas());
    }

    /**
     * Installs a Relation message listener that detects when pgoutput sends a Relation message for
     * a published-but-unaccepted child partition, triggering a session restart.
     */
    private void installPg10RelationListener(
            RelationAwarePostgresSchema schema,
            Pg10CaptureState captureState,
            PostgresOffsetContext currentOffsetContext,
            io.debezium.connector.postgresql.PostgresConnectorConfig connectorConfig,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested,
            AtomicReference<TableId> relationRestartTrigger,
            AtomicReference<String> restartTriggerSource,
            AtomicReference<PostgresOffsetContext> frozenRestartOffset,
            ReplicationConnection replicationConnectionToClose) {
        if (captureState.getParentToChildrenMapping().isEmpty()) {
            return;
        }

        schema.setPartitionListener(
                tableId -> {
                    // This callback runs on the Debezium WAL processing thread. It must remain
                    // lightweight and JDBC-free; expensive verification is deferred until the
                    // subsequent restart reconciliation path.
                    if (refreshRequested.get() || isStopped()) {
                        return;
                    }
                    if (shouldTriggerRestartForRelation(
                            tableId,
                            captureState.getChildToParentMapping(),
                            publishedButUnacceptedChildren)) {
                        if (requestRestart(
                                tableId,
                                "relation",
                                currentOffsetContext,
                                connectorConfig,
                                sessionContext,
                                refreshRequested,
                                relationRestartTrigger,
                                restartTriggerSource,
                                frozenRestartOffset,
                                replicationConnectionToClose)) {
                            LOG.info(
                                    PG10_RELATION_RESTART_REQUESTED_LOG,
                                    getSplit().splitId(),
                                    tableId,
                                    publishedButUnacceptedChildren);
                        }
                    }
                });
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static boolean shouldTriggerRestartForRelation(
            TableId tableId,
            Map<TableId, TableId> acceptedChildToParentMapping,
            Set<TableId> publishedButUnacceptedChildren) {
        return !acceptedChildToParentMapping.containsKey(tableId)
                && publishedButUnacceptedChildren.contains(tableId);
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static PostgresOffsetContext freezeRestartOffsetAtLastCommit(
            PostgresOffsetContext offsetContext,
            io.debezium.connector.postgresql.PostgresConnectorConfig connectorConfig) {
        if (offsetContext == null) {
            return null;
        }

        Map<String, ?> offset = offsetContext.getOffset();
        Long lastCommitLsn = readLong(offset, PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        if (lastCommitLsn == null) {
            return offsetContext;
        }

        Map<String, Object> sanitizedOffset = new HashMap<>(offset);
        Long currentLsn = readLong(offset, SourceInfo.LSN_KEY);
        Long lastCompletelyProcessedLsn =
                readLong(offset, PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        if ((currentLsn != null && currentLsn > lastCommitLsn)
                || (lastCompletelyProcessedLsn != null
                        && lastCompletelyProcessedLsn > lastCommitLsn)) {
            sanitizedOffset.put(SourceInfo.LSN_KEY, lastCommitLsn);
            sanitizedOffset.put(
                    PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCommitLsn);
        }
        return new PostgresOffsetContext.Loader(connectorConfig).load(sanitizedOffset);
    }

    private static boolean requestRestart(
            TableId triggerTable,
            String triggerSource,
            PostgresOffsetContext currentOffsetContext,
            io.debezium.connector.postgresql.PostgresConnectorConfig connectorConfig,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested,
            AtomicReference<TableId> restartTriggerTable,
            AtomicReference<String> restartTriggerSource,
            AtomicReference<PostgresOffsetContext> frozenRestartOffset,
            ReplicationConnection replicationConnectionToClose) {
        if (!refreshRequested.compareAndSet(false, true)) {
            return false;
        }

        restartTriggerTable.compareAndSet(null, triggerTable);
        restartTriggerSource.compareAndSet(null, triggerSource);
        frozenRestartOffset.compareAndSet(
                null, freezeRestartOffsetAtLastCommit(currentOffsetContext, connectorConfig));
        sessionContext.stopChangeEventSource();
        closeReplicationConnectionQuietly(replicationConnectionToClose);
        return true;
    }

    private static void closeReplicationConnectionQuietly(
            ReplicationConnection replicationConnection) {
        if (replicationConnection == null) {
            return;
        }
        try {
            replicationConnection.close();
        } catch (Exception e) {
            LOG.debug("Ignored exception while closing replication connection for PG10 restart", e);
        }
    }

    private static Long readLong(Map<String, ?> offset, String key) {
        Object value = offset.get(key);
        return value == null ? null : ((Number) value).longValue();
    }

    /** Starts a lightweight publication poller for publication maintenance only. */
    private Thread startPg10PublicationPoller(
            PostgresSourceFetchTaskContext sourceFetchContext,
            StoppableChangeEventSourceContext sessionContext,
            AtomicReference<Throwable> pollerFailure,
            AtomicBoolean refreshRequested,
            AtomicReference<TableId> relationRestartTrigger,
            AtomicReference<String> restartTriggerSource,
            AtomicReference<PostgresOffsetContext> frozenRestartOffset,
            PostgresOffsetContext currentOffsetContext,
            ReplicationConnection replicationConnectionToClose) {
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        String publicationName = pgSourceConfig.getDbzProperties().getProperty("publication.name");
        if (!shouldStartPg10PublicationPoller(
                pgSourceConfig, publicationName, currentCaptureState)) {
            return null;
        }

        Thread poller =
                new Thread(
                        () -> {
                            try {
                                pollPg10PublicationMembership(
                                        sourceFetchContext,
                                        publicationName,
                                        sessionContext,
                                        refreshRequested,
                                        relationRestartTrigger,
                                        restartTriggerSource,
                                        frozenRestartOffset,
                                        currentOffsetContext,
                                        replicationConnectionToClose);
                            } catch (Throwable t) {
                                pollerFailure.set(t);
                                sessionContext.stopChangeEventSource();
                            }
                        },
                        "postgres-pg10-publication-poller");
        poller.setDaemon(true);
        this.activePublicationPoller = poller;
        poller.start();
        return poller;
    }

    /**
     * Periodically adds new child partitions to the publication and triggers a fallback restart
     * once published-but-unaccepted children are confirmed visible in the publication.
     *
     * <p>The poller intentionally re-reads {@link #currentCaptureState} on every iteration so it
     * reconciles against the latest accepted capture-state baseline rather than the session-start
     * snapshot.
     */
    private void pollPg10PublicationMembership(
            PostgresSourceFetchTaskContext sourceFetchContext,
            String publicationName,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested,
            AtomicReference<TableId> restartTriggerTable,
            AtomicReference<String> restartTriggerSource,
            AtomicReference<PostgresOffsetContext> frozenRestartOffset,
            PostgresOffsetContext currentOffsetContext,
            ReplicationConnection replicationConnectionToClose) {
        PostgresDialect postgresDialect =
                (PostgresDialect) sourceFetchContext.getDataSourceDialect();
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        List<TableId> parentTables =
                Pg10FetchTaskContextCoordinator.derivePg10ParentTables(
                        getSplit().asStreamSplit(),
                        currentCaptureState,
                        sourceFetchContext.getSourceConfig());
        long pollerStartedAtNanos = System.nanoTime();

        while (!isStopped() && sessionContext.isRunning()) {
            try {
                Thread.sleep(
                        resolvePg10PublicationPollIntervalMillis(
                                pgSourceConfig,
                                elapsedMillisSince(pollerStartedAtNanos, System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            try (PostgresConnection jdbcConnection = postgresDialect.openJdbcConnection()) {
                Pg10CaptureState acceptedCaptureState = currentCaptureState;
                if (acceptedCaptureState == null) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "PG10 accepted capture state is missing while polling "
                                            + "publication membership for split %s",
                                    getSplit()));
                }
                Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                        postgresDialect.reconcilePg10PartitionMappings(
                                jdbcConnection, acceptedCaptureState, parentTables);
                List<TableId> freshlyDiscoveredChildren =
                        getFreshlyDiscoveredChildren(
                                reconcileResult.getNewChildren(), publishedButUnacceptedChildren);
                if (!freshlyDiscoveredChildren.isEmpty()) {
                    LOG.info(
                            PG10_CHILD_DISCOVERED_LOG,
                            getSplit().splitId(),
                            publicationName,
                            freshlyDiscoveredChildren,
                            freshlyDiscoveredChildren.size(),
                            acceptedCaptureState.getChildToParentMapping().size());
                }
                List<TableId> missingPublicationChildren =
                        Pg10PublicationManager.findMissingPublicationChildren(
                                jdbcConnection, publicationName, reconcileResult.getNewChildren());
                if (!missingPublicationChildren.isEmpty()) {
                    Pg10PublicationManager.addTablesToPublication(
                            jdbcConnection, publicationName, missingPublicationChildren);
                } else if (!reconcileResult.getNewChildren().isEmpty()) {
                    LOG.debug(
                            "Publication poller observed PG10 child partitions {} already present "
                                    + "in publication '{}'; requesting fallback restart to accept "
                                    + "a new capture-state baseline.",
                            reconcileResult.getNewChildren(),
                            publicationName);
                }
                overwritePublishedButUnacceptedChildren(reconcileResult.getNewChildren());
                List<TableId> confirmedPublishedButUnacceptedChildren =
                        determineConfirmedPublishedButUnacceptedChildren(
                                jdbcConnection,
                                publicationName,
                                reconcileResult.getNewChildren(),
                                missingPublicationChildren);
                if (shouldTriggerFallbackRestartAfterPublicationRefresh(
                        confirmedPublishedButUnacceptedChildren, refreshRequested)) {
                    TableId triggerTable = confirmedPublishedButUnacceptedChildren.get(0);
                    if (requestRestart(
                            triggerTable,
                            "poller",
                            currentOffsetContext,
                            pgSourceConfig.getDbzConnectorConfig(),
                            sessionContext,
                            refreshRequested,
                            restartTriggerTable,
                            restartTriggerSource,
                            frozenRestartOffset,
                            replicationConnectionToClose)) {
                        LOG.info(
                                PG10_POLL_RESTART_REQUESTED_LOG,
                                getSplit().splitId(),
                                publicationName,
                                triggerTable,
                                confirmedPublishedButUnacceptedChildren,
                                confirmedPublishedButUnacceptedChildren.size());
                    }
                    return;
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to poll PG10 publication membership for split %s",
                                getSplit()),
                        e);
            }
        }
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static boolean shouldStartPg10PublicationPoller(
            PostgresSourceConfig pgSourceConfig,
            String publicationName,
            Pg10CaptureState captureState) {
        return pgSourceConfig.includePartitionedTables()
                && publicationName != null
                && !publicationName.trim().isEmpty()
                && captureState != null
                && !captureState.getParentToChildrenMapping().isEmpty();
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static long resolvePg10PublicationPollIntervalMillis(
            PostgresSourceConfig pgSourceConfig, long elapsedSinceSessionStartMillis) {
        long steadyPollIntervalMillis =
                sanitizePollIntervalMillis(pgSourceConfig.getPg10PublicationPollInterval());
        long startupFastPollDurationMillis =
                Math.max(0L, pgSourceConfig.getPg10StartupFastPollDuration().toMillis());
        if (startupFastPollDurationMillis == 0L
                || elapsedSinceSessionStartMillis >= startupFastPollDurationMillis) {
            return steadyPollIntervalMillis;
        }

        long startupFastPollIntervalMillis =
                sanitizePollIntervalMillis(pgSourceConfig.getPg10StartupFastPollInterval());
        return Math.min(startupFastPollIntervalMillis, steadyPollIntervalMillis);
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static boolean shouldTriggerFallbackRestartAfterPublicationRefresh(
            List<TableId> publishedButUnacceptedChildren, AtomicBoolean refreshRequested) {
        return !publishedButUnacceptedChildren.isEmpty() && !refreshRequested.get();
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static List<TableId> getConfirmedPublishedButUnacceptedChildren(
            List<TableId> newlyDiscoveredChildren, List<TableId> missingPublicationChildren) {
        if (newlyDiscoveredChildren.isEmpty()) {
            return Collections.emptyList();
        }
        if (missingPublicationChildren.isEmpty()) {
            return newlyDiscoveredChildren;
        }

        LinkedHashSet<TableId> confirmedChildren = new LinkedHashSet<>(newlyDiscoveredChildren);
        confirmedChildren.removeAll(missingPublicationChildren);
        return confirmedChildren.isEmpty()
                ? Collections.emptyList()
                : new ArrayList<>(confirmedChildren);
    }

    @org.apache.flink.cdc.common.annotation.VisibleForTesting
    static List<TableId> getFreshlyDiscoveredChildren(
            List<TableId> newlyDiscoveredChildren, Set<TableId> publishedButUnacceptedChildren) {
        if (newlyDiscoveredChildren.isEmpty()) {
            return Collections.emptyList();
        }
        if (publishedButUnacceptedChildren.isEmpty()) {
            return newlyDiscoveredChildren;
        }

        LinkedHashSet<TableId> freshlyDiscoveredChildren =
                new LinkedHashSet<>(newlyDiscoveredChildren);
        freshlyDiscoveredChildren.removeAll(publishedButUnacceptedChildren);
        return freshlyDiscoveredChildren.isEmpty()
                ? Collections.emptyList()
                : new ArrayList<>(freshlyDiscoveredChildren);
    }

    private List<TableId> determineConfirmedPublishedButUnacceptedChildren(
            PostgresConnection jdbcConnection,
            String publicationName,
            List<TableId> newlyDiscoveredChildren,
            List<TableId> initialMissingPublicationChildren)
            throws Exception {
        if (newlyDiscoveredChildren.isEmpty()) {
            return Collections.emptyList();
        }

        if (initialMissingPublicationChildren.isEmpty()) {
            return newlyDiscoveredChildren;
        }

        List<TableId> remainingMissingPublicationChildren =
                Pg10PublicationManager.findMissingPublicationChildren(
                        jdbcConnection, publicationName, newlyDiscoveredChildren);
        return getConfirmedPublishedButUnacceptedChildren(
                newlyDiscoveredChildren, remainingMissingPublicationChildren);
    }

    private static long sanitizePollIntervalMillis(Duration pollInterval) {
        long pollIntervalMillis = pollInterval.toMillis();
        if (pollIntervalMillis <= 0L) {
            throw new IllegalArgumentException(
                    String.format(
                            "PG10 publication poll interval must be greater than 0 ms, but is %s",
                            pollInterval));
        }
        return pollIntervalMillis;
    }

    private static long elapsedMillisSince(long startedAtNanos, long nowNanos) {
        return Math.max(0L, (nowNanos - startedAtNanos) / 1_000_000L);
    }

    private void overwritePublishedButUnacceptedChildren(List<TableId> stillNewChildren) {
        if (stillNewChildren.isEmpty()) {
            publishedButUnacceptedChildren = Collections.emptySet();
            return;
        }

        publishedButUnacceptedChildren =
                Collections.unmodifiableSet(new LinkedHashSet<>(stillNewChildren));
    }

    private void removeAcceptedChildrenFromRestartGate(
            Map<TableId, TableId> acceptedChildToParentMapping) {
        Set<TableId> currentlyPublishedButUnaccepted = publishedButUnacceptedChildren;
        if (currentlyPublishedButUnaccepted.isEmpty()) {
            return;
        }

        LinkedHashSet<TableId> remainingChildren =
                new LinkedHashSet<>(currentlyPublishedButUnaccepted);
        remainingChildren.removeIf(acceptedChildToParentMapping::containsKey);
        publishedButUnacceptedChildren =
                remainingChildren.isEmpty()
                        ? Collections.emptySet()
                        : Collections.unmodifiableSet(remainingChildren);
    }
}
