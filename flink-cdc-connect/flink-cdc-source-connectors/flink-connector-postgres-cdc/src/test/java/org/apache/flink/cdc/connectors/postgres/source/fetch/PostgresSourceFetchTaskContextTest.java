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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;

/** Unit test for {@link PostgresSourceFetchTaskContext}. */
class PostgresSourceFetchTaskContextTest {

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader<PostgresOffsetContext> offsetLoader;

    @BeforeEach
    public void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    void shouldNotResetLsnWhenLastCommitLsnIsNull() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);
        Assertions.assertThat(lastKnownLsn(offsetContext)).isEqualTo(Lsn.valueOf(12345L));
    }

    @Test
    void shouldAcceptRestartOffsetContextWithoutChangingItsLsn() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 12345L);

        final PostgresOffsetContext originalOffsetContext = offsetLoader.load(offsetValues);
        final org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig
                sourceConfig =
                        Mockito.mock(
                                org.apache.flink.cdc.connectors.postgres.source.config
                                        .PostgresSourceConfig.class);
        Mockito.when(sourceConfig.getDbzConnectorConfig()).thenReturn(connectorConfig);
        final PostgresSourceFetchTaskContext fetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, null);

        fetchTaskContext.setRestartOffsetContext(originalOffsetContext);

        Assertions.assertThat(fetchTaskContext.getRestartOffsetContext())
                .isSameAs(originalOffsetContext);
        Assertions.assertThat(lastKnownLsn(fetchTaskContext.getRestartOffsetContext()))
                .isEqualTo(Lsn.valueOf(12345L));
    }

    @Test
    void shouldBuildInitialCaptureStateFromCurrentSourceConfigMappings() {
        TableId parentTable = new TableId(null, "inventory", "products");
        TableId childUk = new TableId(null, "inventory", "products_uk");
        TableId childUs = new TableId(null, "inventory", "products_us");

        Map<TableId, TableId> childToParent = new LinkedHashMap<>();
        childToParent.put(childUk, parentTable);
        childToParent.put(childUs, parentTable);
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(parentTable, new ArrayList<>(List.of(childUk, childUs)));

        PostgresSourceConfig sourceConfig = createSourceConfig();
        sourceConfig.setChildToParentMapping(childToParent);
        sourceConfig.setParentToChildrenMapping(parentToChildren);
        sourceConfig.setPg10PartitionMappingInitialized(true);

        PostgresSourceFetchTaskContext fetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, null);

        Pg10CaptureState initialCaptureState =
                fetchTaskContext.buildInitialCaptureState(createMinimalStreamSplit());

        childToParent.remove(childUs);
        parentToChildren.get(parentTable).remove(childUs);

        Map<TableId, TableId> expectedChildToParent = new LinkedHashMap<>();
        expectedChildToParent.put(childUk, parentTable);
        expectedChildToParent.put(childUs, parentTable);

        Assertions.assertThat(initialCaptureState.getChildToParentMapping())
                .containsExactlyEntriesOf(expectedChildToParent);
        Assertions.assertThat(initialCaptureState.getParentToChildrenMapping())
                .containsEntry(parentTable, List.of(childUk, childUs));
    }

    @Test
    void shouldSyncPg10CaptureStateWithoutReplacingRestartOffset() {
        TableId parentTable = new TableId(null, "inventory", "products");
        TableId childUk = new TableId(null, "inventory", "products_uk");
        TableId childUs = new TableId(null, "inventory", "products_us");

        PostgresSourceConfig sourceConfig = createSourceConfig();
        PostgresSourceFetchTaskContext fetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, null);
        PostgresOffsetContext originalRestartOffsetContext = loadOffsetContext(12345L, 12345L);
        fetchTaskContext.setRestartOffsetContext(originalRestartOffsetContext);

        Pg10CaptureState captureState = createCaptureState(parentTable, List.of(childUk, childUs));

        fetchTaskContext.syncPg10CaptureState(captureState, createMinimalStreamSplit());

        Assertions.assertThat(fetchTaskContext.getRestartOffsetContext())
                .isSameAs(originalRestartOffsetContext);
        Assertions.assertThat(sourceConfig.isPg10PartitionMappingInitialized()).isTrue();
        Assertions.assertThat(sourceConfig.getChildToParentMappingOrEmpty())
                .isEqualTo(captureState.getChildToParentMapping());
        Assertions.assertThat(sourceConfig.getParentToChildrenMappingOrEmpty())
                .isEqualTo(captureState.getParentToChildrenMapping());
        Assertions.assertThat(
                        fetchTaskContext
                                .getDbzConnectorConfig()
                                .getConfig()
                                .getString("table.include.list"))
                .isEqualTo("inventory.products,inventory.products_uk,inventory.products_us");
    }

    @Test
    void shouldAcceptPg10CaptureStateForRestartAndMakeItNextInitialState() {
        TableId parentTable = new TableId(null, "inventory", "products");
        TableId childUk = new TableId(null, "inventory", "products_uk");
        TableId childUs = new TableId(null, "inventory", "products_us");

        PostgresSourceConfig sourceConfig = createSourceConfig();
        PostgresSourceFetchTaskContext fetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, null);
        PostgresOffsetContext restartOffsetContext = loadOffsetContext(67890L, 67890L);
        Pg10CaptureState acceptedCaptureState =
                createCaptureState(parentTable, List.of(childUk, childUs));

        fetchTaskContext.acceptPg10CaptureStateForRestart(
                acceptedCaptureState, restartOffsetContext, createMinimalStreamSplit());

        Assertions.assertThat(fetchTaskContext.getRestartOffsetContext())
                .isSameAs(restartOffsetContext);
        Assertions.assertThat(lastKnownLsn(fetchTaskContext.getRestartOffsetContext()))
                .isEqualTo(Lsn.valueOf(67890L));

        Pg10CaptureState nextInitialCaptureState =
                fetchTaskContext.buildInitialCaptureState(createMinimalStreamSplit());

        Assertions.assertThat(nextInitialCaptureState.getChildToParentMapping())
                .isEqualTo(acceptedCaptureState.getChildToParentMapping());
        Assertions.assertThat(nextInitialCaptureState.getParentToChildrenMapping())
                .isEqualTo(acceptedCaptureState.getParentToChildrenMapping());
    }

    @Test
    void shouldAcceptPg10CaptureStateForRestartFromLastCommittedLsn() {
        TableId parentTable = new TableId(null, "inventory", "products");
        TableId childUk = new TableId(null, "inventory", "products_uk");

        PostgresSourceConfig sourceConfig = createSourceConfig();
        PostgresSourceFetchTaskContext fetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, null);

        PostgresOffsetContext inFlightRestartOffsetContext = loadOffsetContext(200L, 200L, 100L);
        Pg10CaptureState acceptedCaptureState = createCaptureState(parentTable, List.of(childUk));

        fetchTaskContext.acceptPg10CaptureStateForRestart(
                acceptedCaptureState, inFlightRestartOffsetContext, createMinimalStreamSplit());

        Assertions.assertThat(lastKnownLsn(fetchTaskContext.getRestartOffsetContext()))
                .isEqualTo(Lsn.valueOf(100L));
        Assertions.assertThat(
                        fetchTaskContext
                                .getRestartOffsetContext()
                                .getOffset()
                                .get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY))
                .isEqualTo(100L);
    }

    private PostgresOffsetContext loadOffsetContext(Long lsn, Long lastCommitLsn) {
        return loadOffsetContext(lsn, null, lastCommitLsn);
    }

    private PostgresOffsetContext loadOffsetContext(
            Long lsn, Long lastCompletelyProcessedLsn, Long lastCommitLsn) {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, lsn);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        if (lastCompletelyProcessedLsn != null) {
            offsetValues.put(
                    PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY,
                    lastCompletelyProcessedLsn);
        }
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, lastCommitLsn);
        return offsetLoader.load(offsetValues);
    }

    private PostgresSourceConfig createSourceConfig() {
        PostgresSourceConfigFactory sourceConfigFactory = new PostgresSourceConfigFactory();
        sourceConfigFactory.hostname("localhost");
        sourceConfigFactory.port(5432);
        sourceConfigFactory.username("postgres");
        sourceConfigFactory.password("postgres");
        sourceConfigFactory.database("postgres");
        sourceConfigFactory.schemaList(new String[] {"inventory"});
        sourceConfigFactory.tableList("inventory.products");
        sourceConfigFactory.startupOptions(StartupOptions.latest());
        sourceConfigFactory.setIncludePartitionedTables(true);
        return sourceConfigFactory.create(0);
    }

    private StreamSplit createMinimalStreamSplit() {
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID,
                offsetFactory.createInitialOffset(),
                offsetFactory.createNoStoppingOffset(),
                Collections.emptyList(),
                Collections.emptyMap(),
                0);
    }

    private Pg10CaptureState createCaptureState(TableId parentTable, List<TableId> childTables) {
        Map<TableId, TableId> childToParent = new LinkedHashMap<>();
        for (TableId childTable : childTables) {
            childToParent.put(childTable, parentTable);
        }
        return Pg10CaptureState.of(
                childToParent, Collections.singletonMap(parentTable, new ArrayList<>(childTables)));
    }
}
