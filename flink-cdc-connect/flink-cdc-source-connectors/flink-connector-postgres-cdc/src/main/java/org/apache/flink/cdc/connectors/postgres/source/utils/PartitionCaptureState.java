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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of partition routing state.
 *
 * <p>This is the single source of truth for child→parent partition mappings. It is:
 *
 * <ul>
 *   <li>Created during enumerator initialization (discovery phase)
 *   <li>Serialized into StreamSplit for checkpoint persistence
 *   <li>Used by streaming runtime for event routing and partition reconciliation
 * </ul>
 *
 * <p>Instances are immutable. Use {@link #withUpdatedMappings} to create a new state with
 * additional mappings when new partitions are discovered at runtime.
 *
 * <p><b>Memory design</b>: Only {@code parentToChildren} is stored eagerly. The reverse mapping
 * {@code childToParent} is derived lazily on first access and cached, avoiding unnecessary
 * computation and allocation when routing is not active or the state is short-lived (e.g. during
 * {@link #withUpdatedMappings} chains).
 *
 * <p><b>Serialization note</b>: This class implements {@link Serializable} for transport between
 * enumerator and reader (via StreamSplit metadata), but is NOT serialized into Flink checkpoints or
 * savepoints. The state is re-discovered from the database on task restart.
 */
public final class PartitionCaptureState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Empty state singleton for non-partition mode. */
    public static final PartitionCaptureState EMPTY =
            new PartitionCaptureState(Collections.emptyMap(), false, null);

    /** parent → children mapping (the single source of truth). */
    private final Map<TableId, List<TableId>> parentToChildren;

    /** Whether partition routing is active for this split. */
    private final boolean routingEnabled;

    /** The publication name used for partition membership validation. May be null. */
    @Nullable private final String publicationName;

    /** Lazily-derived reverse mapping (child → parent). Volatile for safe publication. */
    private transient volatile Map<TableId, TableId> childToParentCache;

    private PartitionCaptureState(
            Map<TableId, List<TableId>> parentToChildren,
            boolean routingEnabled,
            @Nullable String publicationName) {
        this.parentToChildren = Collections.unmodifiableMap(new HashMap<>(parentToChildren));
        this.routingEnabled = routingEnabled;
        this.publicationName = publicationName;
    }

    /** Convenience factory: creates state from parent→children mapping. */
    public static PartitionCaptureState of(
            Map<TableId, List<TableId>> parentToChildren,
            boolean routingEnabled,
            @Nullable String publicationName) {
        return new PartitionCaptureState(parentToChildren, routingEnabled, publicationName);
    }

    public Map<TableId, List<TableId>> getParentToChildren() {
        return parentToChildren;
    }

    /** Returns child→parent mapping, derived lazily from parentToChildren on first call. */
    public Map<TableId, TableId> getChildToParent() {
        Map<TableId, TableId> cached = childToParentCache;
        if (cached == null) {
            synchronized (this) {
                cached = childToParentCache;
                if (cached == null) {
                    cached =
                            Collections.unmodifiableMap(
                                    PartitionMapper.buildChildToParentMapping(parentToChildren));
                    childToParentCache = cached;
                }
            }
        }
        return cached;
    }

    public boolean isRoutingEnabled() {
        return routingEnabled;
    }

    @Nullable
    public String getPublicationName() {
        return publicationName;
    }

    /**
     * Returns the parent table for a given child, or null if not a known child partition. Used for
     * event routing in the streaming phase.
     */
    @Nullable
    public TableId getParentFor(TableId childTableId) {
        return getChildToParent().get(childTableId);
    }

    /** Returns true if the given tableId is a known child partition. */
    public boolean isChildPartition(TableId tableId) {
        return getChildToParent().containsKey(tableId);
    }

    /** Returns true if the given tableId is a known parent (partitioned) table. */
    public boolean isParentTable(TableId tableId) {
        return parentToChildren.containsKey(tableId);
    }

    /**
     * Creates a new state with additional mappings merged in. Used when new partitions are
     * discovered at runtime. For each parent, new children are appended to the existing list
     * (de-duplicated) rather than replacing it, so previously known children are never lost.
     */
    public PartitionCaptureState withUpdatedMappings(
            Map<TableId, List<TableId>> newParentToChildren) {
        Map<TableId, List<TableId>> merged = new HashMap<>(this.parentToChildren);
        for (Map.Entry<TableId, List<TableId>> entry : newParentToChildren.entrySet()) {
            List<TableId> existing = merged.get(entry.getKey());
            if (existing == null) {
                merged.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            } else {
                // Append only truly new children, preserving existing order
                LinkedHashSet<TableId> combined = new LinkedHashSet<>(existing);
                combined.addAll(entry.getValue());
                merged.put(entry.getKey(), new ArrayList<>(combined));
            }
        }
        return new PartitionCaptureState(merged, this.routingEnabled, this.publicationName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionCaptureState that = (PartitionCaptureState) o;
        return routingEnabled == that.routingEnabled
                && Objects.equals(parentToChildren, that.parentToChildren)
                && Objects.equals(publicationName, that.publicationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentToChildren, routingEnabled, publicationName);
    }

    @Override
    public String toString() {
        // Intentionally O(1) — never enumerate per-table entries. Dumping tens of thousands of
        // child partitions (e.g. time-range daily partitions over years) would explode the
        // taskmanager log and stall diagnostic dump threads. We aggregate to counts only.
        // The int is also clamped to Integer.MAX_VALUE to keep the formatter safe in the
        // (extremely unlikely) case of a partition fan-out >2^31.
        long childCount = parentToChildren.values().stream().mapToLong(List::size).sum();
        int displayedChildCount = (int) Math.min(childCount, Integer.MAX_VALUE);
        return String.format(
                "PartitionCaptureState{routing=%s, parents=%d, children=%d, publication=%s}",
                routingEnabled, parentToChildren.size(), displayedChildCount, publicationName);
    }
}
