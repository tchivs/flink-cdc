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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Immutable snapshot of PostgreSQL partition child-to-parent routing metadata. */
public final class PartitionRoutingState implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final PartitionRoutingState EMPTY =
            new PartitionRoutingState(Collections.emptyMap());

    private final Map<TableId, List<TableId>> parentToChildren;
    private final Map<TableId, TableId> childToParent;

    private PartitionRoutingState(Map<TableId, List<TableId>> parentToChildren) {
        Map<TableId, List<TableId>> parentCopy = new LinkedHashMap<>();
        Map<TableId, TableId> childCopy = new LinkedHashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry : parentToChildren.entrySet()) {
            List<TableId> children =
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue()));
            parentCopy.put(entry.getKey(), children);
            for (TableId child : children) {
                TableId previousParent = childCopy.put(child, entry.getKey());
                if (previousParent != null && !previousParent.equals(entry.getKey())) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Child partition %s belongs to multiple parents: %s and %s",
                                    child, previousParent, entry.getKey()));
                }
            }
        }
        this.parentToChildren = Collections.unmodifiableMap(parentCopy);
        this.childToParent = Collections.unmodifiableMap(childCopy);
    }

    public static PartitionRoutingState of(Map<TableId, List<TableId>> parentToChildren) {
        if (parentToChildren == null || parentToChildren.isEmpty()) {
            return EMPTY;
        }
        return new PartitionRoutingState(parentToChildren);
    }

    public boolean isEmpty() {
        return childToParent.isEmpty();
    }

    public boolean containsParent(TableId tableId) {
        return parentToChildren.containsKey(tableId);
    }

    public boolean containsChild(TableId tableId) {
        return childToParent.containsKey(tableId);
    }

    public TableId routeToLogicalTable(TableId physicalTableId) {
        return childToParent.getOrDefault(physicalTableId, physicalTableId);
    }

    public Optional<TableId> parentOf(TableId childTableId) {
        return Optional.ofNullable(childToParent.get(childTableId));
    }

    public List<TableId> childrenOf(TableId parentTableId) {
        return parentToChildren.getOrDefault(parentTableId, Collections.emptyList());
    }

    public Map<TableId, List<TableId>> parentToChildren() {
        return parentToChildren;
    }

    public Map<TableId, TableId> childToParent() {
        return childToParent;
    }

    public Set<TableId> allChildren() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(childToParent.keySet()));
    }

    public Set<TableId> allParents() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(parentToChildren.keySet()));
    }

    public boolean containsAmbiguousCapture(Collection<TableId> capturedTables) {
        for (TableId parent : parentToChildren.keySet()) {
            if (capturedTables.contains(parent)) {
                for (TableId child : childrenOf(parent)) {
                    if (capturedTables.contains(child)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Set<TableId> findMixedParentChildCaptures(Collection<TableId> capturedTables) {
        Set<TableId> ambiguous = new HashSet<>();
        for (TableId parent : parentToChildren.keySet()) {
            if (!capturedTables.contains(parent)) {
                continue;
            }
            for (TableId child : childrenOf(parent)) {
                if (capturedTables.contains(child)) {
                    ambiguous.add(parent);
                    ambiguous.add(child);
                }
            }
        }
        return ambiguous;
    }

    public PartitionRoutingState merge(Map<TableId, List<TableId>> newMappings) {
        if (newMappings == null || newMappings.isEmpty()) {
            return this;
        }
        Map<TableId, List<TableId>> merged = new LinkedHashMap<>(parentToChildren);
        for (Map.Entry<TableId, List<TableId>> entry : newMappings.entrySet()) {
            LinkedHashSet<TableId> children =
                    new LinkedHashSet<>(
                            merged.getOrDefault(entry.getKey(), Collections.emptyList()));
            children.addAll(entry.getValue());
            merged.put(entry.getKey(), new ArrayList<>(children));
        }
        return of(merged);
    }

    @Nullable
    public TableId firstParentFor(Collection<TableId> capturedTables) {
        for (TableId parent : parentToChildren.keySet()) {
            if (capturedTables.contains(parent)) {
                return parent;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionRoutingState)) {
            return false;
        }
        PartitionRoutingState that = (PartitionRoutingState) o;
        return Objects.equals(parentToChildren, that.parentToChildren);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentToChildren);
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionRoutingState{parents=%d, children=%d}",
                parentToChildren.size(), childToParent.size());
    }
}
