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

package org.apache.flink.cdc.connectors.postgres.source.schema;

import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** Unit tests for {@link RelationAwarePostgresSchema}. */
class RelationAwarePostgresSchemaTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId FILTERED = new TableId(null, "public", "customers");

    @Test
    void buildAndRegisterSchemaAlsoRegistersTableMetadata() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        RelationAwarePostgresSchema schema =
                new RelationAwarePostgresSchema(config, null, null, topicSelector, null);
        Table table = Table.editor().tableId(PARENT).create();

        schema.buildAndRegisterSchema(table);

        Assertions.assertThat(schema.tableFor(PARENT)).isEqualTo(table);
        Assertions.assertThat(schema.schemaFor(PARENT)).isNotNull();
    }

    @Test
    void defaultPathDispatchesSchemaChangesForIncludedTablesOnly() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        List<Table> dispatched = new ArrayList<>();
        schema.setDispatcher(dispatched::add);

        Table includedTable = table(PARENT);
        schema.applySchemaChangesForTable(1, includedTable);

        Assertions.assertThat(schema.tableFor(1)).isEqualTo(includedTable);
        Assertions.assertThat(dispatched).containsExactly(includedTable);

        schema.applySchemaChangesForTable(2, table(FILTERED));

        Assertions.assertThat(dispatched).containsExactly(includedTable);
    }

    private static Table table(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("INTEGER")
                                .jdbcType(java.sql.Types.INTEGER)
                                .position(1)
                                .optional(false)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static class TestPostgresSchema extends RelationAwarePostgresSchema {

        TestPostgresSchema(PostgresConnectorConfig config, TopicSelector<TableId> topicSelector) {
            super(config, null, null, topicSelector, null);
        }

        @Override
        public void buildAndRegisterSchema(Table table) {
            tables().overwriteTable(table);
        }
    }
}
