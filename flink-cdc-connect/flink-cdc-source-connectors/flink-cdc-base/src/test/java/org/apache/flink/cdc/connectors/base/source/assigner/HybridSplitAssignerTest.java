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

package org.apache.flink.cdc.connectors.base.source.assigner;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link HybridSplitAssigner}. */
class HybridSplitAssignerTest {

    @Test
    void testOpenInitializesSnapshotMetricsBeforeStartingSnapshotAssigner() {
        SourceConfig sourceConfig = new TestSourceConfig();
        RecordingSnapshotSplitAssigner snapshotSplitAssigner =
                new RecordingSnapshotSplitAssigner(sourceConfig);
        HybridSplitAssigner<SourceConfig> hybridSplitAssigner =
                new HybridSplitAssigner<>(
                        sourceConfig,
                        snapshotSplitAssigner,
                        false,
                        1,
                        new TestOffsetFactory(),
                        new MockSplitEnumeratorContext<SourceSplitBase>(1));

        hybridSplitAssigner.open();

        assertThat(snapshotSplitAssigner.calls).containsExactly("initEnumeratorMetrics", "open");
    }

    private static class RecordingSnapshotSplitAssigner
            extends SnapshotSplitAssigner<SourceConfig> {
        private final List<String> calls = new ArrayList<>();

        private RecordingSnapshotSplitAssigner(SourceConfig sourceConfig) {
            super(
                    sourceConfig,
                    1,
                    Collections.emptyList(),
                    true,
                    dialect(sourceConfig),
                    new TestOffsetFactory());
        }

        @Override
        public void open() {
            calls.add("open");
        }

        @Override
        public void initEnumeratorMetrics(SourceEnumeratorMetrics enumeratorMetrics) {
            calls.add("initEnumeratorMetrics");
        }

        private static DataSourceDialect<SourceConfig> dialect(SourceConfig sourceConfig) {
            return new TestDataSourceDialect();
        }
    }

    private static class TestSourceConfig implements SourceConfig {
        @Override
        public StartupOptions getStartupOptions() {
            return StartupOptions.initial();
        }

        @Override
        public int getSplitSize() {
            return 1;
        }

        @Override
        public int getSplitMetaGroupSize() {
            return 1;
        }

        @Override
        public boolean isIncludeSchemaChanges() {
            return false;
        }

        @Override
        public boolean isCloseIdleReaders() {
            return false;
        }

        @Override
        public boolean isSkipSnapshotBackfill() {
            return false;
        }

        @Override
        public boolean isScanNewlyAddedTableEnabled() {
            return false;
        }

        @Override
        public boolean isAssignUnboundedChunkFirst() {
            return false;
        }
    }

    private static class TestDataSourceDialect implements DataSourceDialect<SourceConfig> {
        @Override
        public String getName() {
            return "test";
        }

        @Override
        public List<TableId> discoverDataCollections(SourceConfig sourceConfig) {
            return Collections.emptyList();
        }

        @Override
        public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
                SourceConfig sourceConfig) {
            return Collections.emptyMap();
        }

        @Override
        public Offset displayCurrentOffset(SourceConfig sourceConfig) {
            return null;
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(SourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(SourceConfig sourceConfig) {
            return new TestChunkSplitter();
        }

        @Override
        public ChunkSplitter createChunkSplitter(
                SourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
            return new TestChunkSplitter();
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return null;
        }

        @Override
        public FetchTask.Context createFetchTaskContext(SourceConfig sourceConfig) {
            return null;
        }

        @Override
        public boolean isIncludeDataCollection(SourceConfig sourceConfig, TableId tableId) {
            return false;
        }
    }

    private static class TestChunkSplitter implements ChunkSplitter {
        @Override
        public void open() {}

        @Override
        public Collection<SnapshotSplit> generateSplits(TableId tableId) {
            return Collections.emptyList();
        }

        @Override
        public boolean hasNextChunk() {
            return false;
        }

        @Override
        public ChunkSplitterState snapshotState(long checkpointId) {
            return ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
        }

        @Override
        public TableId getCurrentSplittingTableId() {
            return null;
        }

        @Override
        public void close() {}
    }

    private static class TestOffsetFactory extends OffsetFactory {
        @Override
        public Offset newOffset(Map<String, String> offset) {
            return null;
        }

        @Override
        public Offset newOffset(String filename, Long position) {
            return null;
        }

        @Override
        public Offset newOffset(Long position) {
            return null;
        }

        @Override
        public Offset createTimestampOffset(long timestampMillis) {
            return null;
        }

        @Override
        public Offset createInitialOffset() {
            return null;
        }

        @Override
        public Offset createNoStoppingOffset() {
            return null;
        }
    }
}
