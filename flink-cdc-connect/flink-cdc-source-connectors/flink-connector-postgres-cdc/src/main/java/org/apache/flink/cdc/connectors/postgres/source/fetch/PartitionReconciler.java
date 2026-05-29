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

import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight WAL-driven partition reconciler that implements {@link
 * RelationAwarePostgresSchema.NewPartitionListener}.
 *
 * <p>When a Relation message arrives for a table that is NOT in the current partition capture state
 * (neither a known parent nor a known child), it flags a reconciliation as needed. The streaming
 * runtime periodically checks this flag to decide whether a session restart is required to rebuild
 * the partition routing mappings.
 *
 * <p>This class is thread-safe: the {@link #onRelationSeen} method is called from the Debezium WAL
 * thread, while {@link #needsReconciliation()} and {@link #reset()} are called from the Flink task
 * thread.
 */
public class PartitionReconciler implements RelationAwarePostgresSchema.NewPartitionListener {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionReconciler.class);

    private final PartitionCaptureState captureState;
    private final AtomicBoolean reconciliationNeeded = new AtomicBoolean(false);
    private final Set<TableId> unknownRelations = ConcurrentHashMap.newKeySet();

    public PartitionReconciler(PartitionCaptureState captureState) {
        this.captureState = captureState;
    }

    @Override
    public void onRelationSeen(TableId tableId) {
        boolean routing = captureState.isRoutingEnabled();
        boolean isParent = captureState.isParentTable(tableId);
        boolean isChild = captureState.isChildPartition(tableId);
        if (!routing) {
            return;
        }

        // If this table is already a known parent or child, nothing to do
        if (isParent || isChild) {
            return;
        }

        // Unknown table appeared in WAL — might be a new child partition.
        // We don't do JDBC here (WAL thread constraint). Just flag it.
        if (unknownRelations.add(tableId)) {
            LOG.info(
                    "New unknown relation detected in WAL: {}. "
                            + "Partition reconciliation will be triggered.",
                    tableId);
            reconciliationNeeded.set(true);
        }
    }

    /**
     * Returns true if a new unknown relation has been seen since the last reset. The streaming
     * runtime should check this periodically and restart the session if true.
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
