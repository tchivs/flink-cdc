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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;

/** Unit tests for {@link PartitionAwareStreamFetchTask#freezeRestartOffsetAtLastCommit}. */
class PartitionAwareStreamFetchTaskTest {

    private PostgresConnectorConfig connectorConfig;
    private PostgresOffsetContext.Loader offsetLoader;

    @BeforeEach
    void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    void freezeReturnsNullWhenOffsetContextIsNull() {
        Assertions.assertThat(
                        PartitionAwareStreamFetchTask.freezeRestartOffsetAtLastCommit(
                                null, connectorConfig))
                .isNull();
    }

    @Test
    void freezeReturnsSameContextWhenLastCommitLsnIsMissing() {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        // No LAST_COMMIT_LSN_KEY → cannot freeze, should return the same instance.
        PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        PostgresOffsetContext frozen =
                PartitionAwareStreamFetchTask.freezeRestartOffsetAtLastCommit(
                        offsetContext, connectorConfig);

        // Without a commit LSN there is nothing to roll back; same instance is returned.
        Assertions.assertThat(frozen).isSameAs(offsetContext);
    }

    @Test
    void freezeRollsBackCurrentLsnToLastCommitWhenAdvancedBeyondIt() {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 200L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 200L);
        PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        PostgresOffsetContext frozen =
                PartitionAwareStreamFetchTask.freezeRestartOffsetAtLastCommit(
                        offsetContext, connectorConfig);

        // LSN was rolled back from 200 to 100 (last commit) to avoid losing events.
        Assertions.assertThat(lastKnownLsn(frozen)).isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    void freezeKeepsCurrentLsnWhenItDoesNotExceedLastCommit() {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 100L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 100L);
        PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        PostgresOffsetContext frozen =
                PartitionAwareStreamFetchTask.freezeRestartOffsetAtLastCommit(
                        offsetContext, connectorConfig);

        // No drift to fix → LSN stays at 100.
        Assertions.assertThat(lastKnownLsn(frozen)).isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    void freezeRollsBackWhenLastCompletelyProcessedExceedsCommit() {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 100L); // not advanced
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 150L);
        PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        PostgresOffsetContext frozen =
                PartitionAwareStreamFetchTask.freezeRestartOffsetAtLastCommit(
                        offsetContext, connectorConfig);

        // LAST_COMPLETELY_PROCESSED > commit triggers rollback even when LSN is at commit.
        Assertions.assertThat(lastKnownLsn(frozen)).isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    void usesPgoutputOnlyForPgoutputPlugin() {
        Assertions.assertThat(PartitionAwareStreamFetchTask.usesPgoutput(sourceConfig("pgoutput")))
                .isTrue();
        Assertions.assertThat(PartitionAwareStreamFetchTask.usesPgoutput(sourceConfig("PGOUTPUT")))
                .isTrue();
        Assertions.assertThat(
                        PartitionAwareStreamFetchTask.usesPgoutput(sourceConfig("decoderbufs")))
                .isFalse();
    }

    private static PostgresSourceConfig sourceConfig(String pluginName) {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(5432);
        factory.username("postgres");
        factory.password("postgres");
        factory.database("postgres");
        factory.schemaList(new String[] {"public"});
        factory.tableList("public.orders");
        factory.decodingPluginName(pluginName);

        Properties properties = new Properties();
        properties.setProperty("publication.autocreate.mode", "filtered");
        factory.debeziumProperties(properties);
        factory.setIncludePartitionedTables(true);
        return factory.create(0);
    }
}
