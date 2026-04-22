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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The state of split to describe the change log of table(s). */
public class StreamSplitState extends SourceSplitState {

    @Nullable private Offset startingOffset;
    @Nullable private Offset endingOffset;
    private final Map<TableId, TableChange> tableSchemas;
    private final Map<TableId, TableId> pg10ChildToParentMapping;
    private final Map<TableId, List<TableId>> pg10ParentToChildrenMapping;
    private boolean pg10RoutingStateInitialized;

    public StreamSplitState(StreamSplit split) {
        super(split);
        this.startingOffset = split.getStartingOffset();
        this.endingOffset = split.getEndingOffset();
        this.tableSchemas = split.getTableSchemas();
        this.pg10ChildToParentMapping = new LinkedHashMap<>(split.getPg10ChildToParentMapping());
        this.pg10ParentToChildrenMapping =
                deepCopyParentToChildren(split.getPg10ParentToChildrenMapping());
        this.pg10RoutingStateInitialized = split.isPg10RoutingStateInitialized();
    }

    @Nullable
    public Offset getStartingOffset() {
        return startingOffset;
    }

    public void setStartingOffset(@Nullable Offset startingOffset) {
        this.startingOffset = startingOffset;
    }

    @Nullable
    public Offset getEndingOffset() {
        return endingOffset;
    }

    public void setEndingOffset(@Nullable Offset endingOffset) {
        this.endingOffset = endingOffset;
    }

    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public void recordSchema(TableId tableId, TableChange latestTableChange) {
        this.tableSchemas.put(tableId, latestTableChange);
    }

    public Map<TableId, TableId> getPg10ChildToParentMapping() {
        return pg10ChildToParentMapping;
    }

    public Map<TableId, List<TableId>> getPg10ParentToChildrenMapping() {
        return pg10ParentToChildrenMapping;
    }

    public boolean isPg10RoutingStateInitialized() {
        return pg10RoutingStateInitialized;
    }

    public void setPg10RoutingState(
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping,
            boolean pg10RoutingStateInitialized) {
        this.pg10ChildToParentMapping.clear();
        this.pg10ChildToParentMapping.putAll(childToParentMapping);
        this.pg10ParentToChildrenMapping.clear();
        this.pg10ParentToChildrenMapping.putAll(parentToChildrenMapping);
        this.pg10RoutingStateInitialized = pg10RoutingStateInitialized;
    }

    @Override
    public StreamSplit toSourceSplit() {
        final StreamSplit streamSplit = split.asStreamSplit();
        return new StreamSplit(
                streamSplit.splitId(),
                getStartingOffset(),
                getEndingOffset(),
                streamSplit.asStreamSplit().getFinishedSnapshotSplitInfos(),
                getTableSchemas(),
                streamSplit.getTotalFinishedSplitSize(),
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                getPg10ChildToParentMapping(),
                getPg10ParentToChildrenMapping(),
                isPg10RoutingStateInitialized());
    }

    private Map<TableId, List<TableId>> deepCopyParentToChildren(
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        Map<TableId, List<TableId>> copied = new LinkedHashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry : parentToChildrenMapping.entrySet()) {
            copied.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copied;
    }

    @Override
    public String toString() {
        return "StreamSplitState{"
                + "startingOffset="
                + startingOffset
                + ", endingOffset="
                + endingOffset
                + ", split="
                + split
                + '}';
    }
}
