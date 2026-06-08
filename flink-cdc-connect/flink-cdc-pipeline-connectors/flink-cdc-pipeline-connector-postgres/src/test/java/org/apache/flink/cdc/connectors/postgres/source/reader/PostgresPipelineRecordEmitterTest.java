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

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for partition-aware create table event selection in {@link PostgresPipelineRecordEmitter}.
 */
class PostgresPipelineRecordEmitterTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");

    @Test
    void keepsNativeParentSchemaWhenChildrenAreDiscoveredBeforeParent() {
        Map<TableId, TableChange> tableSchemas = new LinkedHashMap<>();
        tableSchemas.put(CHILD_2024, change(ordersTable(CHILD_2024, "child_only")));
        tableSchemas.put(CHILD_2025, change(ordersTable(CHILD_2025, "child_only")));
        tableSchemas.put(PARENT, change(ordersTable(PARENT, "parent_col")));

        Map<TableId, Table> selected =
                PostgresPipelineRecordEmitter.selectCreateTableSchemas(
                        tableSchemas, PostgresPipelineRecordEmitterTest::routeToParent);

        assertThat(selected).containsOnlyKeys(PARENT);
        assertThat(selected.get(PARENT).columnWithName("parent_col")).isNotNull();
        assertThat(selected.get(PARENT).columnWithName("child_only")).isNull();
    }

    @Test
    void deduplicatesChildrenWhenParentSchemaIsUnavailable() {
        Map<TableId, TableChange> tableSchemas = new LinkedHashMap<>();
        tableSchemas.put(CHILD_2024, change(ordersTable(CHILD_2024, "child_col")));
        tableSchemas.put(CHILD_2025, change(ordersTable(CHILD_2025, "child_col")));

        Map<TableId, Table> selected =
                PostgresPipelineRecordEmitter.selectCreateTableSchemas(
                        tableSchemas, PostgresPipelineRecordEmitterTest::routeToParent);

        assertThat(selected).containsOnlyKeys(PARENT);
        assertThat(selected.get(PARENT).id()).isEqualTo(PARENT);
        assertThat(selected.get(PARENT).columnWithName("child_col")).isNotNull();
    }

    @Test
    void keepsOrdinaryTablesUnchanged() {
        TableId ordinary = new TableId(null, "public", "customers");
        Map<TableId, TableChange> tableSchemas =
                Collections.singletonMap(ordinary, change(ordersTable(ordinary, "name")));

        Map<TableId, Table> selected =
                PostgresPipelineRecordEmitter.selectCreateTableSchemas(
                        tableSchemas, PostgresPipelineRecordEmitterTest::routeToParent);

        assertThat(selected).containsOnlyKeys(ordinary);
        assertThat(selected.get(ordinary).id()).isEqualTo(ordinary);
    }

    private static TableId routeToParent(TableId tableId) {
        return Arrays.asList(CHILD_2024, CHILD_2025).contains(tableId) ? PARENT : tableId;
    }

    private static Table ordersTable(TableId tableId, String extraColumn) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(column("id", "integer", Types.INTEGER, 1, false))
                .addColumn(column(extraColumn, "text", Types.VARCHAR, 2, true))
                .setPrimaryKeyNames("id")
                .create();
    }

    private static TableChange change(Table table) {
        return new TableChange(TableChanges.TableChangeType.CREATE, table);
    }

    private static Column column(
            String name, String typeName, int jdbcType, int position, boolean optional) {
        return Column.editor()
                .name(name)
                .type(typeName)
                .jdbcType(jdbcType)
                .position(position)
                .optional(optional)
                .create();
    }
}
