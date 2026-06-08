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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PostgresSourceBuilder}. */
class PostgresSourceBuilderTest {

    @Test
    void restoresStreamOnlyEnumeratorWithPartitionRoutingSeeded() {
        PostgresSourceConfigFactory configFactory = configFactory();
        configFactory.startupOptions(StartupOptions.latest());
        configFactory.setIncludePartitionedTables(true);
        CountingPostgresDialect dialect = new CountingPostgresDialect(configFactory.create(0));
        PostgresSourceBuilder.PostgresIncrementalSource<SourceRecord> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory,
                        new ForwardDeserializeSchema(),
                        new PostgresOffsetFactory(),
                        dialect);

        source.restoreEnumerator(
                new MockSplitEnumeratorContext<>(1), new StreamPendingSplitsState(true));

        assertThat(dialect.discoveryCalls).isEqualTo(1);
    }

    @Test
    void restoresStreamOnlyEnumeratorWithoutPartitionRoutingSeedWhenDisabled() {
        PostgresSourceConfigFactory configFactory = configFactory();
        configFactory.startupOptions(StartupOptions.latest());
        CountingPostgresDialect dialect = new CountingPostgresDialect(configFactory.create(0));
        PostgresSourceBuilder.PostgresIncrementalSource<SourceRecord> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory,
                        new ForwardDeserializeSchema(),
                        new PostgresOffsetFactory(),
                        dialect);

        source.restoreEnumerator(
                new MockSplitEnumeratorContext<>(1), new StreamPendingSplitsState(true));

        assertThat(dialect.discoveryCalls).isZero();
    }

    @Test
    void rejectsUnsupportedDecoderBeforeStreamOnlyPartitionRoutingSeed() {
        PostgresSourceConfigFactory configFactory = configFactory();
        configFactory.startupOptions(StartupOptions.latest());
        configFactory.setIncludePartitionedTables(true);
        configFactory.decodingPluginName("wal2json");
        CountingPostgresDialect dialect = new CountingPostgresDialect(configFactory.create(0));
        PostgresSourceBuilder.PostgresIncrementalSource<SourceRecord> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory,
                        new ForwardDeserializeSchema(),
                        new PostgresOffsetFactory(),
                        dialect);

        assertThatThrownBy(
                        () ->
                                source.restoreEnumerator(
                                        new MockSplitEnumeratorContext<>(1),
                                        new StreamPendingSplitsState(true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wal2json")
                .hasMessageContaining("pgoutput and decoderbufs");
        assertThat(dialect.discoveryCalls).isZero();
    }

    private static PostgresSourceConfigFactory configFactory() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList("public.orders");
        configFactory.username("user");
        configFactory.password("password");
        configFactory.decodingPluginName("pgoutput");
        return configFactory;
    }

    private static class CountingPostgresDialect extends PostgresDialect {

        private int discoveryCalls;

        private CountingPostgresDialect(PostgresSourceConfig sourceConfig) {
            super(sourceConfig);
        }

        @Override
        public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
            discoveryCalls++;
            return Collections.singletonList(new TableId(null, "public", "orders_2025"));
        }
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }
}
