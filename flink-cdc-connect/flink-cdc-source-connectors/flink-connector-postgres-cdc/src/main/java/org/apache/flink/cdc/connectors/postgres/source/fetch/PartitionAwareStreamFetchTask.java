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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /** Tracked so that close() can interrupt it across session restarts. */
    private volatile Thread activePublicationPoller;

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
                                currentOffsetContext);

                try {
                    restartRequired = runStreamingSession(sourceFetchContext, coordinator, runtime);
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
     */
    private boolean runStreamingSession(
            PostgresSourceFetchTaskContext sourceFetchContext,
            PartitionFetchContextCoordinator coordinator,
            StreamingSessionRuntime runtime)
            throws Exception {

        AtomicBoolean refreshRequested = new AtomicBoolean(false);
        AtomicReference<Throwable> pollerFailure = new AtomicReference<>();

        // Install partition reconciler as WAL Relation listener
        PartitionReconciler reconciler = new PartitionReconciler(currentCaptureState);
        runtime.getSchema().setPartitionListener(reconciler);

        // Start publication poller thread
        Thread publicationPoller =
                startPublicationPoller(sourceFetchContext, refreshRequested, pollerFailure);

        // Create and execute the streaming read task
        StoppableChangeEventSourceContext sessionContext = new StoppableChangeEventSourceContext();
        setChangeEventSourceContext(sessionContext);

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

        // Install a periodic check that stops the session when reconciliation is needed
        Thread reconcilerChecker =
                startReconcilerChecker(reconciler, sessionContext, refreshRequested);

        try {
            readTask.execute(sessionContext, runtime.getPartition(), runtime.getOffsetContext());
        } finally {
            // Cleanup - always runs, even when readTask.execute throws, to avoid leaking
            // background threads, listener references, or the activePublicationPoller field.
            cleanupStreamingSession(runtime, publicationPoller, reconcilerChecker);
        }

        if (pollerFailure.get() != null) {
            Throwable cause = pollerFailure.get();
            if (cause instanceof FlinkRuntimeException) {
                throw (FlinkRuntimeException) cause;
            }
            throw new FlinkRuntimeException(
                    "Failed during partition publication polling for split " + getSplit(), cause);
        }

        if (!refreshRequested.get()) {
            return false;
        }

        // Reconcile: rebuild partition mappings
        List<TableId> parentTables =
                PartitionFetchContextCoordinator.deriveParentTables(
                        getSplit().asStreamSplit(), currentCaptureState);

        PartitionCaptureState reconciled =
                coordinator.reconcilePartitionMappings(currentCaptureState, parentTables);
        if (reconciled == currentCaptureState) {
            LOG.info(
                    "Partition reconciliation found no actual changes; "
                            + "resuming without session restart.");
            return false;
        }

        this.currentCaptureState = reconciled;
        // Sync updated state to dialect for emitter-level routing
        ((PostgresDialect) sourceFetchContext.getDataSourceDialect())
                .setCurrentPartitionState(reconciled);
        this.currentOffsetContext =
                freezeRestartOffsetAtLastCommit(
                        runtime.getOffsetContext(), runtime.getDbzConnectorConfig());

        LOG.info(
                "Restarting partition-aware streaming session after detecting new child partitions. "
                        + "New mapping size: {}",
                reconciled.getChildToParent().size());
        return true;
    }

    /**
     * Releases per-session resources: clears the WAL-relation listener, drops the
     * activePublicationPoller field, and interrupts/joins the background threads. Always called
     * from the {@code finally} guarding the streaming read so cleanup runs even when the read
     * threw.
     */
    private void cleanupStreamingSession(
            StreamingSessionRuntime runtime,
            @Nullable Thread publicationPoller,
            @Nullable Thread reconcilerChecker) {
        try {
            runtime.getSchema().setPartitionListener(null);
        } catch (Throwable t) {
            LOG.warn("Failed to clear partition listener during streaming session cleanup.", t);
        }
        activePublicationPoller = null;
        joinThreadQuietly(publicationPoller, "publication poller");
        joinThreadQuietly(reconcilerChecker, "reconciler checker");
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

    /**
     * Starts a thread that periodically checks the PartitionReconciler and stops the streaming
     * session when reconciliation is needed.
     */
    private Thread startReconcilerChecker(
            PartitionReconciler reconciler,
            StoppableChangeEventSourceContext sessionContext,
            AtomicBoolean refreshRequested) {
        if (!currentCaptureState.isRoutingEnabled()) {
            return null;
        }

        Thread checker =
                new Thread(
                        () -> {
                            while (!isStopped() && sessionContext.isRunning()) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }

                                if (reconciler.needsReconciliation()) {
                                    LOG.info(
                                            "Reconciler detected new relations: {}. "
                                                    + "Stopping streaming session for restart.",
                                            reconciler.getUnknownRelations());
                                    refreshRequested.set(true);
                                    sessionContext.stopChangeEventSource();
                                    return;
                                }
                            }
                        },
                        "postgres-partition-reconciler-checker");
        checker.setDaemon(true);
        checker.start();
        return checker;
    }

    /** Starts a lightweight publication poller that adds new children to the publication. */
    private Thread startPublicationPoller(
            PostgresSourceFetchTaskContext sourceFetchContext,
            AtomicBoolean refreshRequested,
            AtomicReference<Throwable> pollerFailure) {
        PostgresSourceConfig pgSourceConfig =
                (PostgresSourceConfig) sourceFetchContext.getSourceConfig();
        String publicationName = pgSourceConfig.getDbzProperties().getProperty("publication.name");
        if (!shouldStartPublicationPoller(pgSourceConfig, publicationName)) {
            return null;
        }

        Thread poller =
                new Thread(
                        () -> {
                            try {
                                pollPublicationMembership(
                                        sourceFetchContext, publicationName, refreshRequested);
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
            String publicationName,
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
                    List<TableId> missingChildren =
                            PublicationManager.findMissingChildren(
                                    jdbc,
                                    publicationName,
                                    currentCaptureState.getChildToParent().keySet());
                    if (!missingChildren.isEmpty()) {
                        LOG.info(
                                "Adding {} new child partition(s) to publication '{}': {}",
                                missingChildren.size(),
                                publicationName,
                                missingChildren);
                        PublicationManager.addTablesToPublication(
                                jdbc, publicationName, missingChildren);
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

    private boolean shouldStartPublicationPoller(
            PostgresSourceConfig pgSourceConfig, String publicationName) {
        return pgSourceConfig.includePartitionedTables()
                && publicationName != null
                && !publicationName.trim().isEmpty()
                && currentCaptureState != null
                && currentCaptureState.isRoutingEnabled();
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

    private static Long readLong(Map<String, ?> offset, String key) {
        Object value = offset.get(key);
        return value == null ? null : ((Number) value).longValue();
    }
}
