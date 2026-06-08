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

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionMapper;
import org.apache.flink.cdc.connectors.postgres.source.utils.PublicationManager;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/**
 * Partition-aware streaming fetch task that supports dynamic child partition discovery and session
 * restart.
 *
 * <p>When partitioned tables are included, new child partitions can be created at runtime. This
 * subclass extends the base streaming task with:
 *
 * <ul>
 *   <li>A <b>session restart loop</b> that rebuilds Debezium runtime objects when new partitions
 *       are discovered.
 *   <li>A <b>Relation message listener</b> ({@link PartitionReconciler}) that detects new child
 *       partitions in the WAL stream with zero latency, leveraging the pgoutput protocol guarantee
 *       that a Relation ('R') message is sent before any DML for a new table.
 *   <li>A <b>lightweight publication poller</b> with configurable intervals that adds newly created
 *       child partitions to the publication via ALTER PUBLICATION ADD TABLE.
 * </ul>
 *
 * <p>For non-partitioned scenarios, use the base {@link PostgresStreamFetchTask} directly.
 */
public class PartitionAwareStreamFetchTask extends PostgresStreamFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareStreamFetchTask.class);

    private volatile PartitionCaptureState currentCaptureState;
    private volatile PostgresOffsetContext currentOffsetContext;

    /**
     * Relations proven unrelated in the previous session and suppressed only in the next session.
     */
    private final Set<TableId> ignoredRelations = ConcurrentHashMap.newKeySet();

    /** Tracked so that close() can interrupt it across session restarts. */
    private volatile Thread activePublicationPoller;

    private static final String DEFAULT_PUBLICATION_NAME = "dbz_publication";
    private static final String PGOUTPUT_PLUGIN = "pgoutput";

    public PartitionAwareStreamFetchTask(StreamSplit streamSplit) {
        super(streamSplit);
    }

    @Override
    public void close() {
        Thread poller = activePublicationPoller;
        if (poller != null) {
            poller.interrupt();
        }
        super.close();
    }

    @Override
    public void execute(Context context) throws Exception {
        if (isStopped()) {
            LOG.debug("PartitionAwareStreamFetchTask for split: {} is already stopped", getSplit());
            return;
        }

        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        PartitionFetchContextCoordinator coordinator =
                new PartitionFetchContextCoordinator(sourceFetchContext);
        PostgresDialect dialect = (PostgresDialect) sourceFetchContext.getDataSourceDialect();

        setTaskRunning(true);
        try {
            currentCaptureState = coordinator.buildInitialCaptureState(getSplit().asStreamSplit());
            // Sync initial state to dialect so emitter can route immediately
            dialect.setCurrentPartitionState(currentCaptureState);
            currentOffsetContext = sourceFetchContext.getOffsetContext();

            boolean restartRequired;
            do {
                if (currentCaptureState == null || currentOffsetContext == null) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Partition streaming session state not initialized for split %s",
                                    getSplit()));
                }

                StreamingSessionRuntime runtime =
                        coordinator.buildStreamingRuntime(
                                getSplit().asStreamSplit(),
                                currentCaptureState,
                                currentOffsetContext,
                                (childId, parentId) ->
                                        onLazyPartitionRouteDiscovered(
                                                sourceFetchContext, childId, parentId));
                PostgresOffsetContext sessionRestartOffsetContext =
                        copyOffsetContext(currentOffsetContext, runtime.getDbzConnectorConfig());

                try {
                    restartRequired =
                            runStreamingSession(
                                    sourceFetchContext,
                                    coordinator,
                                    runtime,
                                    sessionRestartOffsetContext);
                } finally {
                    coordinator.closeStreamingRuntime(runtime);
                }
            } while (restartRequired && !isStopped());
        } finally {
            setTaskRunning(false);
        }
    }

    /**
     * Runs a single streaming session. Returns true if a session restart is required (new
     * partitions discovered).
     *
     * <p>Partition detection is driven by the WAL stream: when {@link PartitionReconciler} sees a
     * Relation message for an unknown table, it immediately stops the session (zero latency). The
     * publication poller thread handles the complementary concern of ensuring new child partitions
     * are added to the publication so pgoutput will send their Relation messages.
     */
    private boolean runStreamingSession(
            PostgresSourceFetchTaskContext sourceFetchContext,
            PartitionFetchContextCoordinator coordinator,
            StreamingSessionRuntime runtime,
            PostgresOffsetContext sessionRestartOffsetContext)
            throws Exception {

        AtomicBoolean refreshRequested = new AtomicBoolean(false);
        AtomicReference<Throwable> pollerFailure = new AtomicReference<>();

        // Install partition reconciler as WAL Relation listener.
        // The reconciler directly stops the session via sessionContext when a new partition
        // is detected, eliminating the need for a separate polling checker thread.
        PartitionReconciler reconciler =
                new PartitionReconciler(
                        currentCaptureState,
                        getSplit().asStreamSplit().getTableSchemas().keySet(),
                        snapshotIgnoredRelationsForNextSession());
        runtime.getSchema().setPartitionListener(reconciler);

        // Create and execute the streaming read task
        StoppableChangeEventSourceContext sessionContext = new StoppableChangeEventSourceContext();
        setChangeEventSourceContext(sessionContext);

        // Wire the reconciler to stop the session directly on new partition detection
        reconciler.setSessionContext(sessionContext);

        // Start publication poller thread
        Thread publicationPoller =
                startPublicationPoller(
                        sourceFetchContext,
                        runtime,
                        sessionContext,
                        refreshRequested,
                        pollerFailure);

        // After building the streaming runtime (which triggers Debezium's initPublication()),
        // re-add known child partitions to the publication. This is critical for FILTERED
        // autocreate mode where initPublication() executes ALTER PUBLICATION SET TABLE with
        // only the parent tables, wiping out previously added children.
        resyncPublicationChildren(sourceFetchContext, runtime);

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

        try {
            readTask.execute(sessionContext, runtime.getPartition(), runtime.getOffsetContext());
        } finally {
            // Cleanup - always runs, even when readTask.execute throws, to avoid leaking
            // background threads, listener references, or the activePublicationPoller field.
            cleanupStreamingSession(runtime, publicationPoller);
        }

        if (pollerFailure.get() != null) {
            Throwable cause = pollerFailure.get();
            if (cause instanceof FlinkRuntimeException) {
                throw (FlinkRuntimeException) cause;
            }
            throw new FlinkRuntimeException(
                    "Failed during partition publication polling for split " + getSplit(), cause);
        }

        if (!refreshRequested.get() && !reconciler.needsReconciliation()) {
            return false;
        }

        if (refreshRequested.get() && !reconciler.needsReconciliation()) {
            this.currentOffsetContext =
                    freezeRestartOffsetAtLastCommit(
                            runtime.getOffsetContext(), runtime.getDbzConnectorConfig());
            LOG.info(
                    "Restarting partition-aware streaming session after publication poller "
                            + "refreshed child partition routing. New mapping size: {}",
                    currentCaptureState.getChildToParent().size());
            return true;
        }

        // Reconcile: use incremental approach when possible (reconciler detected specific
        // unknown relations) to avoid a full pg_inherits scan of all parent tables.
        Set<TableId> unknownRelations = reconciler.getUnknownRelations();
        PartitionCaptureState reconciled;
        if (!unknownRelations.isEmpty()) {
            List<TableId> parentTables =
                    PartitionFetchContextCoordinator.deriveParentTables(
                            getSplit().asStreamSplit(), currentCaptureState);
            PartitionFetchContextCoordinator.IncrementalReconciliationResult result =
                    coordinator.incrementalReconcile(
                            currentCaptureState, unknownRelations, parentTables);
            ignoredRelations.addAll(result.getUnrelatedRelations());
            ignoredRelations.removeAll(result.getCaptureState().getChildToParent().keySet());
            if (LOG.isDebugEnabled() && !result.getUnrelatedRelations().isEmpty()) {
                LOG.debug(
                        "Reconciliation marked {} relation(s) as unrelated and added to "
                                + "ignoredRelations: {}",
                        result.getUnrelatedRelations().size(),
                        result.getUnrelatedRelations());
            }
            reconciled = result.getCaptureState();
        } else {
            // Fallback to full reconciliation (e.g. triggered by publication poller)
            List<TableId> parentTables =
                    PartitionFetchContextCoordinator.deriveParentTables(
                            getSplit().asStreamSplit(), currentCaptureState);
            reconciled = coordinator.reconcilePartitionMappings(currentCaptureState, parentTables);
        }

        if (reconciled == currentCaptureState) {
            // No actual partition mapping changes, but the current session was already stopped
            // by the reconciler. We must still restart to resume WAL consumption.
            LOG.info(
                    "Partition reconciliation found no actual changes; "
                            + "restarting streaming session to resume WAL consumption.");
            this.currentOffsetContext =
                    freezeRestartOffsetAtLastCommit(
                            getPartitionReconciliationRestartOffset(
                                    readTask, sessionRestartOffsetContext, runtime),
                            runtime.getDbzConnectorConfig());
            return true;
        }

        this.currentCaptureState = reconciled;
        // Sync updated state to dialect for emitter-level routing
        ((PostgresDialect) sourceFetchContext.getDataSourceDialect())
                .setCurrentPartitionState(reconciled);
        this.currentOffsetContext =
                freezeRestartOffsetAtLastCommit(
                        getPartitionReconciliationRestartOffset(
                                readTask, sessionRestartOffsetContext, runtime),
                        runtime.getDbzConnectorConfig());

        LOG.info(
                "Restarting partition-aware streaming session after detecting new child partitions. "
                        + "New mapping size: {}",
                reconciled.getChildToParent().size());
        return true;
    }

    private void onLazyPartitionRouteDiscovered(
            PostgresSourceFetchTaskContext sourceFetchContext, TableId childId, TableId parentId) {
        Map<TableId, List<TableId>> discovered = new HashMap<>();
        discovered.put(parentId, Collections.singletonList(childId));
        PartitionCaptureState updated =
                updateCaptureStateAtomically(discovered, sourceFetchContext);
        if (updated == null) {
            return;
        }
        LOG.info(
                "Updated partition routing state after lazy child discovery: {} -> {}. "
                        + "New mapping size: {}",
                childId,
                parentId,
                updated.getChildToParent().size());
    }

    /**
     * Releases per-session resources: clears the WAL-relation listener, drops the
     * activePublicationPoller field, and interrupts/joins the background thread. Always called from
     * the {@code finally} guarding the streaming read so cleanup runs even when the read threw.
     */
    private void cleanupStreamingSession(
            StreamingSessionRuntime runtime, @Nullable Thread publicationPoller) {
        try {
            runtime.getSchema().setPartitionListener(null);
        } catch (Throwable t) {
            LOG.warn("Failed to clear partition listener during streaming session cleanup.", t);
        }
        activePublicationPoller = null;
        joinThreadQuietly(publicationPoller, "publication poller");
    }

    private static void joinThreadQuietly(@Nullable Thread thread, String name) {
        if (thread == null) {
            return;
        }
        thread.interrupt();
        try {
            thread.join(5000);
        } catch (InterruptedException ie) {
            // Preserve interrupt status; do not let cleanup mask the original failure.
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while joining {} during streaming session cleanup.", name);
        }
    }

    /** Starts a lightweight publication poller that adds new children to the publication. */
    private Thread startPublicationPoller(
            PostgresSourceFetchTaskContext sourceFetchContext,
            StreamingSessionRuntime runtime,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested,
            AtomicReference<Throwable> pollerFailure) {
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        String publicationName = resolvePublicationName(pgSourceConfig);
        if (!shouldStartPublicationPoller(pgSourceConfig, publicationName)) {
            return null;
        }

        Thread poller =
                new Thread(
                        () -> {
                            try {
                                pollPublicationMembership(
                                        sourceFetchContext,
                                        runtime,
                                        publicationName,
                                        sessionContext,
                                        refreshRequested);
                            } catch (Throwable t) {
                                pollerFailure.set(t);
                            }
                        },
                        "postgres-partition-publication-poller");
        poller.setDaemon(true);
        this.activePublicationPoller = poller;
        poller.start();
        return poller;
    }

    /**
     * Periodically checks for new child partitions and adds them to the publication. This ensures
     * pgoutput will send Relation messages for new children, allowing the reconciler to detect
     * them.
     */
    private void pollPublicationMembership(
            PostgresSourceFetchTaskContext sourceFetchContext,
            StreamingSessionRuntime runtime,
            String publicationName,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested) {
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        long pollIntervalMillis =
                Math.max(1000L, pgSourceConfig.getPartitionDiscoveryPollInterval().toMillis());

        PostgresConnection jdbc = null;
        try {
            while (!isStopped() && !refreshRequested.get()) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                try {
                    if (jdbc == null || !jdbc.isConnected()) {
                        if (jdbc != null) {
                            jdbc.close();
                        }
                        jdbc =
                                new PostgresConnection(
                                        sourceFetchContext.getDbzConnectorConfig().getJdbcConfig(),
                                        newPostgresValueConverterBuilder(
                                                sourceFetchContext.getDbzConnectorConfig()),
                                        "postgres-partition-poll");
                    }
                    List<TableId> parentTables =
                            PartitionFetchContextCoordinator.deriveParentTables(
                                    getSplit().asStreamSplit(), currentCaptureState);
                    Map<TableId, List<TableId>> latestParentToChildren =
                            PartitionMapper.discoverPartitionMappings(parentTables, jdbc, true);
                    List<TableId> latestChildren =
                            PartitionMapper.getAllChildTableIds(latestParentToChildren);
                    List<TableId> unknownChildren =
                            findChildrenUnknownToCurrentState(latestChildren, currentCaptureState);
                    if (!unknownChildren.isEmpty()) {
                        // Merge via withUpdatedMappings instead of full replacement to preserve any
                        // child partitions discovered by the WAL thread
                        // (onLazyPartitionRouteDiscovered) between our JDBC snapshot and now.
                        PartitionCaptureState refreshedCaptureState =
                                updateCaptureStateAtomically(
                                        latestParentToChildren, sourceFetchContext);
                        if (refreshedCaptureState == null) {
                            refreshedCaptureState = currentCaptureState;
                        }
                        if (refreshedCaptureState == null) {
                            continue;
                        }
                        ignoredRelations.removeAll(
                                refreshedCaptureState.getChildToParent().keySet());
                        runtime.getDispatcher()
                                .updatePartitionRouting(refreshedCaptureState.getChildToParent());
                        LOG.info(
                                "Detected {} new child partition(s): {}. Refreshed partition "
                                        + "routing state before refreshing publication '{}' membership.",
                                unknownChildren.size(),
                                unknownChildren,
                                publicationName);
                    }

                    List<TableId> missingChildren =
                            PublicationManager.findMissingChildren(
                                    jdbc, publicationName, latestChildren);
                    if (!missingChildren.isEmpty()) {
                        LOG.info(
                                "Adding {} missing child partition(s) to publication '{}': {}",
                                missingChildren.size(),
                                publicationName,
                                missingChildren);
                        PublicationManager.addTablesToPublication(
                                jdbc, publicationName, missingChildren);
                    }
                    if (!unknownChildren.isEmpty()) {
                        refreshRequested.set(true);
                        stopStreamingSession(sessionContext, runtime);
                        return;
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to poll/update publication membership for '{}': {}",
                            publicationName,
                            e.getMessage());
                    // Close broken connection so it gets re-created next cycle
                    if (jdbc != null) {
                        try {
                            jdbc.close();
                        } catch (Exception ignored) {
                        }
                        jdbc = null;
                    }
                }
            }
        } finally {
            if (jdbc != null) {
                try {
                    jdbc.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static List<TableId> findChildrenUnknownToCurrentState(
            List<TableId> latestChildren, @Nullable PartitionCaptureState currentCaptureState) {
        List<TableId> unknownChildren = new ArrayList<>();
        for (TableId child : latestChildren) {
            if (currentCaptureState == null || !currentCaptureState.isChildPartition(child)) {
                unknownChildren.add(child);
            }
        }
        return unknownChildren;
    }

    private Set<TableId> snapshotIgnoredRelationsForNextSession() {
        Set<TableId> ignoredSnapshot = ConcurrentHashMap.newKeySet();
        ignoredSnapshot.addAll(ignoredRelations);
        ignoredRelations.clear();
        return ignoredSnapshot;
    }

    /**
     * Atomically merges new partition mappings into {@link #currentCaptureState} and propagates the
     * result to the dialect. Synchronized to prevent read-modify-write races between the WAL thread
     * ({@link #onLazyPartitionRouteDiscovered}) and the publication poller thread ({@link
     * #pollPublicationMembership}).
     *
     * @return the updated state, or {@code null} if {@code currentCaptureState} was null or the
     *     merge produced no changes
     */
    private synchronized PartitionCaptureState updateCaptureStateAtomically(
            Map<TableId, List<TableId>> newParentToChildren,
            PostgresSourceFetchTaskContext sourceFetchContext) {
        PartitionCaptureState snapshot = currentCaptureState;
        if (snapshot == null) {
            return null;
        }
        PartitionCaptureState updated = snapshot.withUpdatedMappings(newParentToChildren);
        if (updated.equals(snapshot)) {
            return null;
        }
        currentCaptureState = updated;
        ((PostgresDialect) sourceFetchContext.getDataSourceDialect())
                .setCurrentPartitionState(updated);
        return updated;
    }

    private void stopStreamingSession(
            StoppableChangeEventSourceContext sessionContext, StreamingSessionRuntime runtime) {
        sessionContext.stopChangeEventSource();
        try {
            runtime.getReplicationConnection().close();
        } catch (Exception e) {
            LOG.warn("Failed to close replication connection while stopping streaming session", e);
        }
    }

    private boolean shouldStartPublicationPoller(
            PostgresSourceConfig pgSourceConfig, String publicationName) {
        if (!pgSourceConfig.includePartitionedTables()
                || publicationName == null
                || publicationName.trim().isEmpty()
                || currentCaptureState == null
                || !currentCaptureState.isRoutingEnabled()) {
            return false;
        }
        if (!usesPgoutput(pgSourceConfig)) {
            LOG.debug(
                    "Skipping publication poller: decoding plugin '{}' does not use PostgreSQL "
                            + "publications.",
                    pgSourceConfig.getDbzProperties().getProperty("plugin.name"));
            return false;
        }
        // Check publication.autocreate.mode: when ALL_TABLES, every table is automatically in
        // the publication so the poller is unnecessary.
        PostgresConnectorConfig.AutoCreateMode autocreateMode = parseAutocreateMode(pgSourceConfig);
        if (autocreateMode == PostgresConnectorConfig.AutoCreateMode.ALL_TABLES) {
            LOG.debug(
                    "Skipping publication poller: autocreate mode is ALL_TABLES, "
                            + "all tables are already in the publication.");
            return false;
        }
        return true;
    }

    static boolean usesPgoutput(PostgresSourceConfig pgSourceConfig) {
        return PGOUTPUT_PLUGIN.equalsIgnoreCase(
                pgSourceConfig.getDbzProperties().getProperty("plugin.name"));
    }

    /**
     * Reads the {@code publication.autocreate.mode} Debezium property and parses it into the
     * strongly-typed {@link PostgresConnectorConfig.AutoCreateMode} enum. Centralised here because
     * the same lookup-and-parse pattern is used by {@link #shouldStartPublicationPoller} and {@link
     * #resyncPublicationChildren}; a single source of truth keeps the default value ("all_tables")
     * and parsing fallback consistent.
     */
    private static PostgresConnectorConfig.AutoCreateMode parseAutocreateMode(
            PostgresSourceConfig pgSourceConfig) {
        String autocreateModeValue =
                pgSourceConfig
                        .getDbzProperties()
                        .getProperty("publication.autocreate.mode", "all_tables");
        return PostgresConnectorConfig.AutoCreateMode.parse(autocreateModeValue, "all_tables");
    }

    /**
     * Re-adds known child partitions to the publication after a session restart. This closes the
     * event-loss window caused by Debezium's {@code initPublication()} in FILTERED autocreate mode,
     * which executes {@code ALTER PUBLICATION SET TABLE} with only the parent tables, wiping out
     * children that the poller had previously added.
     *
     * <p>For ALL_TABLES mode this is a no-op (no children need explicit addition). For DISABLED
     * mode this is skipped (the user manages the publication).
     */
    private void resyncPublicationChildren(
            PostgresSourceFetchTaskContext sourceFetchContext, StreamingSessionRuntime runtime) {
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        if (!usesPgoutput(pgSourceConfig)) {
            return;
        }
        PostgresConnectorConfig.AutoCreateMode autocreateMode = parseAutocreateMode(pgSourceConfig);

        // Only FILTERED mode resets publication members on reconnection.
        // ALL_TABLES includes everything; DISABLED/NO_TABLES don't touch the publication.
        if (autocreateMode != PostgresConnectorConfig.AutoCreateMode.FILTERED) {
            return;
        }

        Set<TableId> knownChildren = currentCaptureState.getChildToParent().keySet();
        if (knownChildren.isEmpty()) {
            return;
        }

        String publicationName = resolvePublicationName(pgSourceConfig);

        try {
            PublicationManager.addTablesToPublication(
                    runtime.getJdbcConnection(), publicationName, new ArrayList<>(knownChildren));
            LOG.info(
                    "Re-synced {} known child partition(s) to publication '{}' "
                            + "after session restart (FILTERED mode).",
                    knownChildren.size(),
                    publicationName);
        } catch (SQLException e) {
            // Non-fatal: the publication poller will eventually add the children.
            // Log a warning so the gap is visible in operational logs. The narrow catch
            // ensures unexpected runtime errors (NPEs, OOMs, IllegalStateException, ...)
            // still propagate and are not silently swallowed.
            LOG.warn(
                    "Failed to re-sync child partitions to publication '{}' after session "
                            + "restart. The publication poller will retry. Error: {}",
                    publicationName,
                    e.getMessage());
        }
    }

    /**
     * Freezes the restart offset at the last committed LSN to ensure we don't miss any events
     * during the session restart gap.
     */
    static PostgresOffsetContext freezeRestartOffsetAtLastCommit(
            PostgresOffsetContext offsetContext, PostgresConnectorConfig connectorConfig) {
        if (offsetContext == null) {
            return null;
        }

        Map<String, ?> offset = offsetContext.getOffset();
        Long lastCommitLsn = readLong(offset, PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        if (lastCommitLsn == null) {
            return offsetContext;
        }

        Map<String, Object> sanitizedOffset = new HashMap<>(offset);
        sanitizedOffset.put(SourceInfo.LSN_KEY, lastCommitLsn);
        sanitizedOffset.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCommitLsn);
        return new PostgresOffsetContext.Loader(connectorConfig).load(sanitizedOffset);
    }

    private static PostgresOffsetContext copyOffsetContext(
            PostgresOffsetContext offsetContext, PostgresConnectorConfig connectorConfig) {
        if (offsetContext == null) {
            return null;
        }
        return new PostgresOffsetContext.Loader(connectorConfig)
                .load(new HashMap<>(offsetContext.getOffset()));
    }

    private static PostgresOffsetContext getPartitionReconciliationRestartOffset(
            StreamSplitReadTask readTask,
            PostgresOffsetContext fallbackOffsetContext,
            StreamingSessionRuntime runtime) {
        Map<String, ?> restartOffset = readTask.getPartitionReconciliationRestartOffset();
        if (restartOffset == null) {
            return fallbackOffsetContext;
        }
        Map<String, Object> sanitizedOffset = new HashMap<>(restartOffset);
        sanitizedOffset.remove(PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        sanitizedOffset.remove(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        return new PostgresOffsetContext.Loader(runtime.getDbzConnectorConfig())
                .load(sanitizedOffset);
    }

    private static Long readLong(Map<String, ?> offset, String key) {
        Object value = offset.get(key);
        return value == null ? null : ((Number) value).longValue();
    }

    static String resolvePublicationName(PostgresSourceConfig pgSourceConfig) {
        return pgSourceConfig
                .getDbzProperties()
                .getProperty("publication.name", DEFAULT_PUBLICATION_NAME);
    }
}
