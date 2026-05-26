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

import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresEventDeserializer;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresPipelineRecordEmitter}. */
class PostgresPipelineRecordEmitterTest {

    @Test
    void testRouteTableIdUsesCurrentPg10ChildToParentMapping() {
        PostgresSourceConfig sourceConfig = createSourceConfig();
        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childTable = new TableId(null, "inventory_partitioned", "products_ca");
        sourceConfig.setChildToParentMapping(Collections.singletonMap(childTable, parentTable));

        TestingPostgresPipelineRecordEmitter emitter =
                new TestingPostgresPipelineRecordEmitter(sourceConfig);

        assertThat(emitter.route(childTable)).isEqualTo(parentTable);
        assertThat(emitter.route(parentTable)).isEqualTo(parentTable);
    }

    @Test
    void testRouteTableIdReflectsUpdatedPg10ChildToParentMapping() {
        PostgresSourceConfig sourceConfig = createSourceConfig();
        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        TableId childAu = new TableId(null, "inventory_partitioned", "products_au");
        sourceConfig.setChildToParentMapping(Collections.singletonMap(childCa, parentTable));

        TestingPostgresPipelineRecordEmitter emitter =
                new TestingPostgresPipelineRecordEmitter(sourceConfig);

        assertThat(emitter.route(childAu)).isEqualTo(childAu);

        sourceConfig.setChildToParentMapping(Map.of(childCa, parentTable, childAu, parentTable));

        assertThat(emitter.route(childAu)).isEqualTo(parentTable);
    }

    private static PostgresSourceConfig createSourceConfig() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"inventory_partitioned"});
        configFactory.tableList("inventory_partitioned.products");
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.decodingPluginName("pgoutput");
        configFactory.slotName("test_slot");
        configFactory.setIncludePartitionedTables(true);
        return configFactory.create(0);
    }

    private static class TestingPostgresPipelineRecordEmitter
            extends PostgresPipelineRecordEmitter<org.apache.flink.cdc.common.event.Event> {

        private TestingPostgresPipelineRecordEmitter(PostgresSourceConfig sourceConfig) {
            super(
                    new PostgresEventDeserializer(DebeziumChangelogMode.ALL),
                    new SourceReaderMetrics(new TestingReaderContext().metricGroup()),
                    sourceConfig,
                    new PostgresOffsetFactory(),
                    new PostgresDialect(sourceConfig));
        }

        private TableId route(TableId tableId) {
            return routeTableId(tableId);
        }
    }
}
