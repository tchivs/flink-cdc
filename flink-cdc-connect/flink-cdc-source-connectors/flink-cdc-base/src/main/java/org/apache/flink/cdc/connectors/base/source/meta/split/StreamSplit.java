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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** The split to describe the change log of database table(s). */
public class StreamSplit extends SourceSplitBase {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSplit.class);
    public static final String STREAM_SPLIT_ID = "stream-split";

    private final Offset startingOffset;
    private final Offset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChange> tableSchemas;
    private final int totalFinishedSplitSize;

    private final boolean isSuspended;

    private final Map<TableId, TableId> pg10ChildToParentMapping;

    private final Map<TableId, List<TableId>> pg10ParentToChildrenMapping;

    private final boolean pg10RoutingStateInitialized;

    /**
     * Indicates whether initial state snapshot was completed right before this split. See
     * io.debezium.connector.sqlserver.SqlServerOffsetContext#isSnapshotCompleted().
     */
    private final boolean isSnapshotCompleted;

    @Nullable transient byte[] serializedFormCache;

    public StreamSplit(
            String splitId,
            Offset startingOffset,
            Offset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted) {
        this(
                splitId,
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended,
                isSnapshotCompleted,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public StreamSplit(
            String splitId,
            Offset startingOffset,
            Offset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted,
            Map<TableId, TableId> pg10ChildToParentMapping,
            Map<TableId, List<TableId>> pg10ParentToChildrenMapping) {
        this(
                splitId,
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended,
                isSnapshotCompleted,
                pg10ChildToParentMapping,
                pg10ParentToChildrenMapping,
                !pg10ChildToParentMapping.isEmpty() || !pg10ParentToChildrenMapping.isEmpty());
    }

    public StreamSplit(
            String splitId,
            Offset startingOffset,
            Offset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted,
            Map<TableId, TableId> pg10ChildToParentMapping,
            Map<TableId, List<TableId>> pg10ParentToChildrenMapping,
            boolean pg10RoutingStateInitialized) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
        this.isSnapshotCompleted = isSnapshotCompleted;
        this.pg10ChildToParentMapping =
                Collections.unmodifiableMap(new LinkedHashMap<>(pg10ChildToParentMapping));
        this.pg10ParentToChildrenMapping =
                Collections.unmodifiableMap(deepCopyParentToChildren(pg10ParentToChildrenMapping));
        this.pg10RoutingStateInitialized = pg10RoutingStateInitialized;
    }

    public StreamSplit(
            String splitId,
            Offset startingOffset,
            Offset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize) {
        this(
                splitId,
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                false,
                false,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public Offset getStartingOffset() {
        return startingOffset;
    }

    public Offset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }

    public boolean isSuspended() {
        return isSuspended;
    }

    public boolean isSnapshotCompleted() {
        return isSnapshotCompleted;
    }

    public Map<TableId, TableId> getPg10ChildToParentMapping() {
        return pg10ChildToParentMapping;
    }

    public Map<TableId, List<TableId>> getPg10ParentToChildrenMapping() {
        return pg10ParentToChildrenMapping;
    }

    public boolean hasPg10RoutingState() {
        return !pg10ChildToParentMapping.isEmpty() || !pg10ParentToChildrenMapping.isEmpty();
    }

    public boolean isPg10RoutingStateInitialized() {
        return pg10RoutingStateInitialized;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StreamSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StreamSplit that = (StreamSplit) o;
        return isSuspended == that.isSuspended
                && isSnapshotCompleted == that.isSnapshotCompleted
                && totalFinishedSplitSize == that.totalFinishedSplitSize
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas)
                && pg10RoutingStateInitialized == that.pg10RoutingStateInitialized
                && Objects.equals(pg10ChildToParentMapping, that.pg10ChildToParentMapping)
                && Objects.equals(pg10ParentToChildrenMapping, that.pg10ParentToChildrenMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended,
                isSnapshotCompleted,
                pg10RoutingStateInitialized,
                pg10ChildToParentMapping,
                pg10ParentToChildrenMapping);
    }

    @Override
    public String toString() {
        return "StreamSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + ", isSnapshotCompleted="
                + isSnapshotCompleted
                + ", pg10RoutingStateInitialized="
                + pg10RoutingStateInitialized
                + ", pg10ChildToParentMapping="
                + pg10ChildToParentMapping
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new StreamSplit instance
    // -------------------------------------------------------------------
    public static StreamSplit appendFinishedSplitInfos(
            StreamSplit streamSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        // re-calculate the starting changelog offset after the new table added
        Offset startingOffset = streamSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }
        splitInfos.addAll(streamSplit.getFinishedSnapshotSplitInfos());

        return new StreamSplit(
                streamSplit.splitId,
                startingOffset,
                streamSplit.getEndingOffset(),
                splitInfos,
                streamSplit.getTableSchemas(),
                streamSplit.getTotalFinishedSplitSize(),
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                streamSplit.getPg10ChildToParentMapping(),
                streamSplit.getPg10ParentToChildrenMapping(),
                streamSplit.isPg10RoutingStateInitialized());
    }

    /**
     * Appends compensating finished split infos to a stream split and increases the finished-split
     * size accordingly.
     *
     * <p>This is used for bounded compensation flows that reuse snapshot/backfill mechanics after a
     * stream split has already been created. Unlike {@link #appendFinishedSplitInfos(StreamSplit,
     * List)}, these finished split infos are not part of the original enumerator metadata groups,
     * so the total finished-split size must grow as new compensation splits are accepted.
     */
    public static StreamSplit appendCompensationSplitInfos(
            StreamSplit streamSplit,
            List<FinishedSnapshotSplitInfo> splitInfos,
            Map<TableId, TableChange> additionalTableSchemas) {
        if (splitInfos.isEmpty() && additionalTableSchemas.isEmpty()) {
            return streamSplit;
        }

        Map<String, FinishedSnapshotSplitInfo> mergedFinishedSplitInfos = new LinkedHashMap<>();
        for (FinishedSnapshotSplitInfo existingInfo : streamSplit.getFinishedSnapshotSplitInfos()) {
            mergedFinishedSplitInfos.put(existingInfo.getSplitId(), existingInfo);
        }

        int newlyAcceptedSplitCount = 0;
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (!mergedFinishedSplitInfos.containsKey(splitInfo.getSplitId())) {
                newlyAcceptedSplitCount++;
            }
            mergedFinishedSplitInfos.put(splitInfo.getSplitId(), splitInfo);
        }

        Offset startingOffset = streamSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : mergedFinishedSplitInfos.values()) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }

        Map<TableId, TableChange> mergedTableSchemas =
                new LinkedHashMap<>(streamSplit.getTableSchemas());
        mergedTableSchemas.putAll(additionalTableSchemas);

        return new StreamSplit(
                streamSplit.splitId,
                startingOffset,
                streamSplit.getEndingOffset(),
                new ArrayList<>(mergedFinishedSplitInfos.values()),
                mergedTableSchemas,
                Math.max(
                                streamSplit.getTotalFinishedSplitSize(),
                                streamSplit.getFinishedSnapshotSplitInfos().size())
                        + newlyAcceptedSplitCount,
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                streamSplit.getPg10ChildToParentMapping(),
                streamSplit.getPg10ParentToChildrenMapping(),
                streamSplit.isPg10RoutingStateInitialized());
    }

    /**
     * Filter out the outdated finished splits in {@link StreamSplit}.
     *
     * <p>When restore from a checkpoint, the finished split infos may contain some splits from the
     * deleted tables. We need to remove these splits from the total finished split infos and update
     * the size.
     */
    public static StreamSplit filterOutdatedSplitInfos(
            StreamSplit streamSplit, Predicate<TableId> currentTableFilter) {

        Set<TableId> tablesToRemove =
                streamSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(i -> !currentTableFilter.test(i.getTableId()))
                        .map(split -> split.getTableId())
                        .collect(Collectors.toSet());
        if (tablesToRemove.isEmpty()) {
            return streamSplit;
        }

        LOG.info("Reader remove tables after restart: {}", tablesToRemove);
        List<FinishedSnapshotSplitInfo> allFinishedSnapshotSplitInfos =
                streamSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(i -> !tablesToRemove.contains(i.getTableId()))
                        .collect(Collectors.toList());
        Map<TableId, TableChange> previousTableSchemas = streamSplit.getTableSchemas();
        Map<TableId, TableChange> newTableSchemas = new HashMap<>();
        previousTableSchemas.keySet().stream()
                .forEach(
                        (tableId -> {
                            if (currentTableFilter.test(tableId)) {
                                newTableSchemas.put(tableId, previousTableSchemas.get(tableId));
                            }
                        }));

        return new StreamSplit(
                streamSplit.splitId,
                streamSplit.getStartingOffset(),
                streamSplit.getEndingOffset(),
                allFinishedSnapshotSplitInfos,
                newTableSchemas,
                streamSplit.getTotalFinishedSplitSize()
                        - (streamSplit.getFinishedSnapshotSplitInfos().size()
                                - allFinishedSnapshotSplitInfos.size()),
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                streamSplit.getPg10ChildToParentMapping(),
                streamSplit.getPg10ParentToChildrenMapping(),
                streamSplit.isPg10RoutingStateInitialized());
    }

    public static StreamSplit fillTableSchemas(
            StreamSplit streamSplit, Map<TableId, TableChange> tableSchemas) {
        tableSchemas.putAll(streamSplit.getTableSchemas());
        return new StreamSplit(
                streamSplit.splitId,
                streamSplit.getStartingOffset(),
                streamSplit.getEndingOffset(),
                streamSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                streamSplit.getTotalFinishedSplitSize(),
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                streamSplit.getPg10ChildToParentMapping(),
                streamSplit.getPg10ParentToChildrenMapping(),
                streamSplit.isPg10RoutingStateInitialized());
    }

    public static StreamSplit toNormalStreamSplit(
            StreamSplit suspendedStreamSplit, int totalFinishedSplitSize) {
        return new StreamSplit(
                suspendedStreamSplit.splitId,
                suspendedStreamSplit.getStartingOffset(),
                suspendedStreamSplit.getEndingOffset(),
                suspendedStreamSplit.getFinishedSnapshotSplitInfos(),
                suspendedStreamSplit.getTableSchemas(),
                totalFinishedSplitSize,
                false,
                suspendedStreamSplit.isSnapshotCompleted(),
                suspendedStreamSplit.getPg10ChildToParentMapping(),
                suspendedStreamSplit.getPg10ParentToChildrenMapping(),
                suspendedStreamSplit.isPg10RoutingStateInitialized());
    }

    public static StreamSplit toSuspendedStreamSplit(StreamSplit normalStreamSplit) {
        return new StreamSplit(
                normalStreamSplit.splitId,
                normalStreamSplit.getStartingOffset(),
                normalStreamSplit.getEndingOffset(),
                forwardHighWatermarkToStartingOffset(
                        normalStreamSplit.getFinishedSnapshotSplitInfos(),
                        normalStreamSplit.getStartingOffset()),
                normalStreamSplit.getTableSchemas(),
                normalStreamSplit.getTotalFinishedSplitSize(),
                true,
                normalStreamSplit.isSnapshotCompleted(),
                normalStreamSplit.getPg10ChildToParentMapping(),
                normalStreamSplit.getPg10ParentToChildrenMapping(),
                normalStreamSplit.isPg10RoutingStateInitialized());
    }

    public static StreamSplit withPg10RoutingState(
            StreamSplit streamSplit,
            Map<TableId, TableId> pg10ChildToParentMapping,
            Map<TableId, List<TableId>> pg10ParentToChildrenMapping) {
        return withPg10RoutingState(
                streamSplit, pg10ChildToParentMapping, pg10ParentToChildrenMapping, true);
    }

    public static StreamSplit withPg10RoutingState(
            StreamSplit streamSplit,
            Map<TableId, TableId> pg10ChildToParentMapping,
            Map<TableId, List<TableId>> pg10ParentToChildrenMapping,
            boolean pg10RoutingStateInitialized) {
        return new StreamSplit(
                streamSplit.splitId,
                streamSplit.getStartingOffset(),
                streamSplit.getEndingOffset(),
                streamSplit.getFinishedSnapshotSplitInfos(),
                streamSplit.getTableSchemas(),
                streamSplit.getTotalFinishedSplitSize(),
                streamSplit.isSuspended(),
                streamSplit.isSnapshotCompleted(),
                pg10ChildToParentMapping,
                pg10ParentToChildrenMapping,
                pg10RoutingStateInitialized);
    }

    /**
     * Forwards {@link FinishedSnapshotSplitInfo#getHighWatermark()} to current change log reading
     * offset for these snapshot-splits have started the change log reading, this is pretty useful
     * for newly added table process that we can continue to consume change log for these splits
     * from the updated high watermark.
     *
     * @param existedSplitInfos
     * @param currentReadingOffset
     */
    private static List<FinishedSnapshotSplitInfo> forwardHighWatermarkToStartingOffset(
            List<FinishedSnapshotSplitInfo> existedSplitInfos, Offset currentReadingOffset) {
        List<FinishedSnapshotSplitInfo> updatedSnapshotSplitInfos = new ArrayList<>();
        for (FinishedSnapshotSplitInfo existedSplitInfo : existedSplitInfos) {
            // for split has started read stream, forward its high watermark to current stream
            // reading offset
            if (existedSplitInfo.getHighWatermark().isBefore(currentReadingOffset)) {
                FinishedSnapshotSplitInfo forwardHighWatermarkSnapshotSplitInfo =
                        new FinishedSnapshotSplitInfo(
                                existedSplitInfo.getTableId(),
                                existedSplitInfo.getSplitId(),
                                existedSplitInfo.getSplitStart(),
                                existedSplitInfo.getSplitEnd(),
                                currentReadingOffset,
                                existedSplitInfo.getOffsetFactory());
                updatedSnapshotSplitInfos.add(forwardHighWatermarkSnapshotSplitInfo);
            } else {
                updatedSnapshotSplitInfos.add(existedSplitInfo);
            }
        }
        return updatedSnapshotSplitInfos;
    }

    private static Map<TableId, List<TableId>> deepCopyParentToChildren(
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        Map<TableId, List<TableId>> copied = new LinkedHashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry : parentToChildrenMapping.entrySet()) {
            copied.put(
                    entry.getKey(),
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        return copied;
    }
}
