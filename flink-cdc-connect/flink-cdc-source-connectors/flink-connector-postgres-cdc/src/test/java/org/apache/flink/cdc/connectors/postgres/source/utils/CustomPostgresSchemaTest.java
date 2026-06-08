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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for partition-aware schema fallback in {@link CustomPostgresSchema}. */
class CustomPostgresSchemaTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");

    @Test
    void keepsNativeParentSchemaWhenParentHasPrimaryKey() {
        Tables tables = new Tables();
        tables.overwriteTable(ordersTable(PARENT));
        tables.overwriteTable(ordersTable(CHILD_2024));
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024)));

        Table resolved =
                CustomPostgresSchema.resolveTableSchema(PARENT, tables, routingState, null);

        assertThat(resolved.id()).isEqualTo(PARENT);
        assertThat(resolved.primaryKeyColumnNames()).containsExactly("id");
    }

    @Test
    void keepsNativeParentSchemaWhenConfiguredChunkKeyExistsWithoutPrimaryKey() {
        Tables tables = new Tables();
        tables.overwriteTable(ordersTable(PARENT).edit().setPrimaryKeyNames().create());
        tables.overwriteTable(ordersTable(CHILD_2024));
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024)));

        Table resolved =
                CustomPostgresSchema.resolveTableSchema(PARENT, tables, routingState, "id");

        assertThat(resolved.id()).isEqualTo(PARENT);
        assertThat(resolved.primaryKeyColumnNames()).isEmpty();
        assertThat(resolved.columnWithName("id")).isNotNull();
    }

    @Test
    void synthesizesParentSchemaFromChildrenWhenParentSchemaIsMissingKey() {
        Tables tables = new Tables();
        tables.overwriteTable(ordersTable(PARENT).edit().setPrimaryKeyNames().create());
        tables.overwriteTable(ordersTable(CHILD_2024));
        tables.overwriteTable(ordersTable(CHILD_2025));
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        Table resolved =
                CustomPostgresSchema.resolveTableSchema(PARENT, tables, routingState, null);

        assertThat(resolved.id()).isEqualTo(PARENT);
        assertThat(resolved.primaryKeyColumnNames()).containsExactly("id");
        assertThat(resolved.retrieveColumnNames()).containsExactly("id", "name");
    }

    @Test
    void failsFastWhenChildSchemasForFallbackAreIncompatible() {
        Tables tables = new Tables();
        tables.overwriteTable(ordersTable(PARENT).edit().setPrimaryKeyNames().create());
        tables.overwriteTable(ordersTable(CHILD_2024));
        tables.overwriteTable(
                ordersTable(CHILD_2025)
                        .edit()
                        .addColumn(column("region", "text", Types.VARCHAR, 3, true))
                        .create());
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThatThrownBy(
                        () ->
                                CustomPostgresSchema.resolveTableSchema(
                                        PARENT, tables, routingState, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition schema");
    }

    @Test
    void failsFastWhenSynthesizedChildrenDoNotProvideUsablePrimaryKey() {
        Tables tables = new Tables();
        tables.overwriteTable(ordersTable(PARENT).edit().setPrimaryKeyNames().create());
        tables.overwriteTable(ordersTable(CHILD_2024).edit().setPrimaryKeyNames().create());
        tables.overwriteTable(ordersTable(CHILD_2025).edit().setPrimaryKeyNames().create());
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThatThrownBy(
                        () ->
                                CustomPostgresSchema.resolveTableSchema(
                                        PARENT, tables, routingState, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not provide primary-key metadata");
    }

    private static Table ordersTable(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(column("id", "integer", Types.INTEGER, 1, false))
                .addColumn(column("name", "text", Types.VARCHAR, 2, true))
                .setPrimaryKeyNames("id")
                .create();
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
