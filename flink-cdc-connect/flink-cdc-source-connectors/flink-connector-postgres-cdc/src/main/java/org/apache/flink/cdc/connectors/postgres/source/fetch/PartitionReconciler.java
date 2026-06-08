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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight WAL-driven partition reconciler that implements {@link
 * RelationAwarePostgresSchema.NewPartitionListener}.
 *
 * <p>When a Relation message arrives for a table that is neither monitored by the stream split, in
 * the one-shot suppression set, nor present in the current partition capture state, it immediately
 * stops the streaming session via the {@link StoppableChangeEventSourceContext}. This zero-latency
 * approach avoids the overhead of a separate polling thread, allowing the streaming runtime to
 * restart and rebuild partition routing mappings as soon as a new partition is detected in the WAL
 * stream.
 *
 * <p>This class is thread-safe: the {@link #onRelationSeen} method is called from the Debezium WAL
 * thread, while {@link #setSessionContext} is called from the Flink task thread during session
 * setup.
 */
public class PartitionReconciler implements RelationAwarePostgresSchema.NewPartitionListener {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionReconciler.class);

    private final PartitionCaptureState captureState;
    private final Set<TableId> monitoredTables;
    private final Set<TableId> ignoredRelations;
    private final AtomicBoolean reconciliationNeeded = new AtomicBoolean(false);
    private final Set<TableId> unknownRelations = ConcurrentHashMap.newKeySet();

    /**
     * Session context used to stop the streaming session immediately when a new partition is
     * detected. Set by the task thread before the WAL stream starts; read by the WAL thread in
     * {@link #onRelationSeen}. Volatile for safe cross-thread publication.
     */
    private volatile StoppableChangeEventSourceContext sessionContext;

    @VisibleForTesting
    public PartitionReconciler(PartitionCaptureState captureState) {
        this(captureState, Collections.emptySet());
    }

    @VisibleForTesting
    public PartitionReconciler(
            PartitionCaptureState captureState, Collection<TableId> monitoredTables) {
        this(captureState, monitoredTables, Collections.emptySet());
    }

    /**
     * Constructs a reconciler with a one-shot suppression set from the previous streaming session.
     *
     * <p>The supplied collections are copied because the WAL thread mutates {@code
     * ignoredRelations} by removing suppressed entries after use.
     */
    public PartitionReconciler(
            PartitionCaptureState captureState,
            Collection<TableId> monitoredTables,
            Set<TableId> ignoredRelations) {
        this.captureState = captureState;
        this.monitoredTables =
                monitoredTables == null
                        ? Collections.emptySet()
                        : Collections.unmodifiableSet(new HashSet<>(monitoredTables));
        this.ignoredRelations =
                ignoredRelations == null ? new HashSet<>() : new HashSet<>(ignoredRelations);
    }

    /**
     * Sets the session context so that {@link #onRelationSeen} can stop the streaming session
     * immediately upon detecting a new unknown relation, eliminating polling latency.
     */
    public void setSessionContext(StoppableChangeEventSourceContext context) {
        this.sessionContext = context;
    }

    @Override
    public void onRelationSeen(TableId tableId) {
        if (!captureState.isRoutingEnabled()) {
            return;
        }

        // If this table is already monitored or known in the partition mapping, nothing to do.
        // Monitored tables include ordinary non-partitioned tables in mixed table.include.list
        // deployments; those Relation messages must not trigger partition reconciliation.
        if (monitoredTables.contains(tableId)
                || captureState.isParentTable(tableId)
                || captureState.isChildPartition(tableId)) {
            ignoredRelations.remove(tableId);
            return;
        }

        // This relation was already resolved as unrelated in the previous session. Suppress it
        // once so the restarted stream can move past the same Relation message/LSN, then drop the
        // entry so any future Relation for the same table is reconsidered.
        //
        // Assumption: pgoutput emits a Relation message at most once per table per session (it is
        // re-emitted only on schema change). If this invariant is violated we may bounce through
        // one extra reconcile-restart cycle, which is benign but wasteful.
        if (ignoredRelations.remove(tableId)) {
            return;
        }

        // Unknown table appeared in WAL — might be a new child partition.
        // We don't do JDBC here (WAL thread constraint). Just flag it and stop the session.
        if (unknownRelations.add(tableId)) {
            LOG.info(
                    "New unknown relation detected in WAL: {}. "
                            + "Stopping streaming session immediately for partition reconciliation.",
                    tableId);
            reconciliationNeeded.set(true);
            // Direct stop: zero-latency session termination from the WAL thread.
            // StoppableChangeEventSourceContext.stopChangeEventSource() is thread-safe
            // (sets a volatile boolean), so calling from the WAL thread is safe.
            StoppableChangeEventSourceContext ctx = this.sessionContext;
            if (ctx != null) {
                ctx.stopChangeEventSource();
            }
            throw new PartitionReconciliationRequiredException(tableId);
        }
    }

    /**
     * Returns true if a new unknown relation has been seen since the last reset. The streaming
     * runtime should check this after the session ends to decide whether reconciliation is needed.
     */
    public boolean needsReconciliation() {
        return reconciliationNeeded.get();
    }

    /** Returns the set of unknown relation table IDs seen since the last reset. */
    public Set<TableId> getUnknownRelations() {
        return unknownRelations;
    }

    /** Resets the reconciliation state after a successful session restart. */
    public void reset() {
        reconciliationNeeded.set(false);
        unknownRelations.clear();
    }
}
