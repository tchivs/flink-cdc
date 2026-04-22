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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitAckEvent;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitEvent;
import org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10CaptureState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The multi-parallel postgres source reader for table snapshot phase from {@link SnapshotSplit} and
 * then single-parallel source reader for table stream phase from {@link StreamSplit}.
 *
 * <p>If scan newly added table is enable, postgres reader will not commit offset until all new
 * added tables' snapshot splits are finished.
 */
@Experimental
public class PostgresSourceReader extends IncrementalSourceReaderWithCommit {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceReader.class);

    @VisibleForTesting static volatile Consumer<StreamSplit> snapshotStreamSplitHook;

    /** whether to commit offset. */
    private volatile boolean isCommitOffset = false;

    private final PriorityQueue<Long> minHeap;
    private final int lsnCommitCheckpointsDelay;

    public PostgresSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            SourceConfig sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
        this.lsnCommitCheckpointsDelay =
                ((PostgresSourceConfig) sourceConfig).getLsnCommitCheckpointsDelay();
        this.minHeap = new PriorityQueue<>();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof OffsetCommitEvent) {
            isCommitOffset = ((OffsetCommitEvent) sourceEvent).isCommitOffset();
            context.sendSourceEventToCoordinator(new OffsetCommitAckEvent());
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void addSplits(List splits) {
        List<SourceSplitBase> splitsWithRestoredPg10State = new ArrayList<>(splits.size());
        for (Object splitObject : splits) {
            SourceSplitBase split = (SourceSplitBase) splitObject;
            splitsWithRestoredPg10State.add(applyCurrentPg10RoutingStateToSplit(split));
        }
        super.addSplits(splitsWithRestoredPg10State);
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (recordEmitter instanceof IncrementalSourceRecordEmitter) {
            ((IncrementalSourceRecordEmitter) recordEmitter).applySplit(split);
        }
        restorePg10RoutingState(split);
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new StreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    protected void updateStreamSplitFinishedSplitsSize(
            LatestFinishedSplitsNumberEvent sourceEvent) {
        super.updateStreamSplitFinishedSplitsSize(sourceEvent);
        isCommitOffset = true;
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);
        mergePg10CompensationStateIntoStreamSplits(sourceSplitBases);
        syncPg10RoutingStateIntoStreamSplits(sourceSplitBases);
        notifySnapshotStreamSplitHook(sourceSplitBases);
        if (!isCommitOffset()) {
            LOG.debug("Close offset commit of checkpoint {}", checkpointId);
            lastCheckpointOffsets.remove(checkpointId);
        }

        return sourceSplitBases;
    }

    @VisibleForTesting
    public static void setSnapshotStreamSplitHook(Consumer<StreamSplit> snapshotStreamSplitHook) {
        PostgresSourceReader.snapshotStreamSplitHook = snapshotStreamSplitHook;
    }

    @VisibleForTesting
    public static void clearSnapshotStreamSplitHook() {
        PostgresSourceReader.snapshotStreamSplitHook = null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.minHeap.add(checkpointId);
        if (this.minHeap.size() <= this.lsnCommitCheckpointsDelay) {
            LOG.info("Pending checkpoints '{}'.", this.minHeap);
            return;
        }
        final long checkpointIdToCommit = this.minHeap.poll();
        LOG.info(
                "Pending checkpoints '{}', to be committed checkpoint id '{}', isCommitOffset is {}.",
                this.minHeap,
                checkpointIdToCommit,
                isCommitOffset());

        // After all snapshot splits are finished, update stream split's metadata and reset start
        // offset, which maybe smaller than before.
        // In case that new start-offset of stream split has been recycled, don't commit offset
        // during new table added phase.
        if (isCommitOffset()) {
            super.notifyCheckpointComplete(checkpointIdToCommit);
        }
    }

    @Override
    protected void onSplitFinished(Map finishedSplitIds) {
        syncRuntimePg10RoutingStateIntoFinishedSplitStates(finishedSplitIds);
        super.onSplitFinished(finishedSplitIds);
        for (Object splitState : finishedSplitIds.values()) {
            SourceSplitBase sourceSplit = ((SourceSplitState) splitState).toSourceSplit();
            if (sourceSplit.isStreamSplit()) {
                StreamSplit streamSplit = sourceSplit.asStreamSplit();
                if (this.sourceConfig.getStartupOptions().isSnapshotOnly()
                        && streamSplit
                                .getStartingOffset()
                                .isAtOrAfter(streamSplit.getEndingOffset())) {
                    PostgresDialect dialect = (PostgresDialect) this.dialect;
                    boolean removed = dialect.removeSlot(dialect.getSlotName());
                    LOG.info("Remove slot '{}' result is {}.", dialect.getSlotName(), removed);
                }
            }
        }
    }

    private boolean isCommitOffset() {
        // Only if scan newly added table is enable, offset commit is controlled by isCommitOffset.
        return !sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset;
    }

    private void restorePg10RoutingState(SourceSplitBase split) {
        if (!split.isStreamSplit()) {
            return;
        }

        StreamSplit streamSplit = split.asStreamSplit();
        if (!streamSplit.isPg10RoutingStateInitialized()) {
            return;
        }

        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        postgresSourceConfig.setChildToParentMapping(streamSplit.getPg10ChildToParentMapping());
        postgresSourceConfig.setParentToChildrenMapping(
                streamSplit.getPg10ParentToChildrenMapping());
        postgresSourceConfig.setPg10PartitionMappingInitialized(true);
        if (dialect instanceof PostgresDialect) {
            ((PostgresDialect) dialect)
                    .syncPg10PartitionMappings(
                            streamSplit.getPg10ChildToParentMapping(),
                            streamSplit.getPg10ParentToChildrenMapping());
        }
    }

    private SourceSplitBase applyCurrentPg10RoutingStateToSplit(SourceSplitBase split) {
        if (!split.isStreamSplit()) {
            return split;
        }

        StreamSplit streamSplit = split.asStreamSplit();
        if (streamSplit.isPg10RoutingStateInitialized()) {
            restorePg10RoutingState(streamSplit);
            return streamSplit;
        }

        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        if (postgresSourceConfig.isPg10PartitionMappingInitialized()) {
            return StreamSplit.withPg10RoutingState(
                    streamSplit,
                    postgresSourceConfig.getChildToParentMappingOrEmpty(),
                    postgresSourceConfig.getParentToChildrenMappingOrEmpty(),
                    true);
        }

        return split;
    }

    private void syncRuntimePg10RoutingStateIntoFinishedSplitStates(Map finishedSplitIds) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        if (!postgresSourceConfig.isPg10PartitionMappingInitialized()) {
            return;
        }

        for (Object splitStateObject : finishedSplitIds.values()) {
            syncRuntimePg10RoutingStateIntoSplitState(splitStateObject, postgresSourceConfig);
        }
    }

    private void syncRuntimePg10RoutingStateIntoSplitState(
            Object splitStateObject, PostgresSourceConfig postgresSourceConfig) {
        SourceSplitState splitState = (SourceSplitState) splitStateObject;
        if (!splitState.isStreamSplitState()) {
            return;
        }
        splitState
                .asStreamSplitState()
                .setPg10RoutingState(
                        postgresSourceConfig.getChildToParentMappingOrEmpty(),
                        postgresSourceConfig.getParentToChildrenMappingOrEmpty(),
                        true);
    }

    private void syncPg10RoutingStateIntoStreamSplits(List<SourceSplitBase> splits) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        if (!postgresSourceConfig.isPg10PartitionMappingInitialized()
                && postgresSourceConfig.getChildToParentMappingOrEmpty().isEmpty()
                && postgresSourceConfig.getParentToChildrenMappingOrEmpty().isEmpty()) {
            return;
        }

        Pg10CaptureState captureState =
                Pg10CaptureState.of(
                        postgresSourceConfig.getChildToParentMappingOrEmpty(),
                        postgresSourceConfig.getParentToChildrenMappingOrEmpty());
        for (int i = 0; i < splits.size(); i++) {
            SourceSplitBase split = splits.get(i);
            if (!split.isStreamSplit()) {
                continue;
            }
            splits.set(
                    i,
                    StreamSplit.withPg10RoutingState(
                            split.asStreamSplit(),
                            captureState.getChildToParentMapping(),
                            captureState.getParentToChildrenMapping(),
                            postgresSourceConfig.isPg10PartitionMappingInitialized()));
        }
    }

    private void mergePg10CompensationStateIntoStreamSplits(List<SourceSplitBase> splits) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        List<FinishedSnapshotSplitInfo> compensationSplitInfos =
                postgresSourceConfig.getPg10CompensationFinishedSplitInfosOrEmpty();
        Map<TableId, TableChanges.TableChange> compensationTableSchemas =
                postgresSourceConfig.getPg10CompensationTableSchemasOrEmpty();
        if (compensationSplitInfos.isEmpty() && compensationTableSchemas.isEmpty()) {
            return;
        }

        for (int i = 0; i < splits.size(); i++) {
            SourceSplitBase split = splits.get(i);
            if (!split.isStreamSplit()) {
                continue;
            }

            StreamSplit mergedStreamSplit =
                    mergePg10CompensationStateIntoStreamSplit(
                            split.asStreamSplit(),
                            compensationSplitInfos,
                            compensationTableSchemas.values());
            splits.set(i, mergedStreamSplit);
        }
    }

    private void notifySnapshotStreamSplitHook(List<SourceSplitBase> splits) {
        Consumer<StreamSplit> hook = snapshotStreamSplitHook;
        if (hook == null) {
            return;
        }

        for (SourceSplitBase split : splits) {
            if (split.isStreamSplit()) {
                hook.accept(split.asStreamSplit());
            }
        }
    }

    private StreamSplit mergePg10CompensationStateIntoStreamSplit(
            StreamSplit streamSplit,
            List<FinishedSnapshotSplitInfo> compensationSplitInfos,
            Collection<TableChanges.TableChange> compensationTableSchemas) {
        Map<String, FinishedSnapshotSplitInfo> existingFinishedInfos = new LinkedHashMap<>();
        for (FinishedSnapshotSplitInfo existingInfo : streamSplit.getFinishedSnapshotSplitInfos()) {
            existingFinishedInfos.put(existingInfo.getSplitId(), existingInfo);
        }

        List<FinishedSnapshotSplitInfo> missingCompensationInfos = new ArrayList<>();
        for (FinishedSnapshotSplitInfo compensationSplitInfo : compensationSplitInfos) {
            if (!existingFinishedInfos.containsKey(compensationSplitInfo.getSplitId())) {
                missingCompensationInfos.add(compensationSplitInfo);
            }
        }

        Map<TableId, TableChanges.TableChange> additionalTableSchemas = new LinkedHashMap<>();
        for (TableChanges.TableChange tableChange : compensationTableSchemas) {
            if (!streamSplit.getTableSchemas().containsKey(tableChange.getId())) {
                additionalTableSchemas.put(tableChange.getId(), tableChange);
            }
        }

        if (missingCompensationInfos.isEmpty() && additionalTableSchemas.isEmpty()) {
            return streamSplit;
        }

        return StreamSplit.appendCompensationSplitInfos(
                streamSplit, missingCompensationInfos, additionalTableSchemas);
    }
}
