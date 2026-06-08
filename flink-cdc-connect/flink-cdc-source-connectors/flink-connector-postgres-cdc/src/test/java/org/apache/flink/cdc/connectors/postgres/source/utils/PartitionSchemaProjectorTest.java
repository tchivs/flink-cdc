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
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionSchemaProjector}. */
class PartitionSchemaProjectorTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");

    @Test
    void synthesizesParentSchemaFromCompatibleChildren() {
        Table synthesized =
                PartitionSchemaProjector.synthesizeFromChildren(
                        PARENT,
                        Arrays.asList(
                                change(ordersTable(CHILD_2024)), change(ordersTable(CHILD_2025))));

        assertThat(synthesized.id()).isEqualTo(PARENT);
        assertThat(synthesized.retrieveColumnNames()).containsExactly("id", "name");
        assertThat(synthesized.primaryKeyColumnNames()).containsExactly("id");
        assertThat(synthesized.columnWithName("name").isOptional()).isTrue();
    }

    @Test
    void rejectsChildrenWithDifferentColumnOrder() {
        Table reorderedChild =
                Table.editor()
                        .tableId(CHILD_2025)
                        .addColumn(column("name", "text", Types.VARCHAR, 1, true))
                        .addColumn(column("id", "integer", Types.INTEGER, 2, false))
                        .setPrimaryKeyNames("id")
                        .create();

        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT,
                                        Arrays.asList(
                                                change(ordersTable(CHILD_2024)),
                                                change(reorderedChild))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition schema")
                .hasMessageContaining("columns");
    }

    @Test
    void rejectsChildrenWithDifferentPrimaryKeyShape() {
        Table childWithCompositePk =
                ordersTable(CHILD_2025).edit().setPrimaryKeyNames("id", "name").create();

        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT,
                                        Arrays.asList(
                                                change(ordersTable(CHILD_2024)),
                                                change(childWithCompositePk))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition primary key");
    }

    @Test
    void rejectsChildrenWithDifferentPrimaryKeyNullability() {
        Table childWithNullablePk =
                Table.editor()
                        .tableId(CHILD_2025)
                        .addColumn(column("id", "integer", Types.INTEGER, 1, true))
                        .addColumn(column("name", "text", Types.VARCHAR, 2, true))
                        .setPrimaryKeyNames("id")
                        .create();

        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT,
                                        Arrays.asList(
                                                change(ordersTable(CHILD_2024)),
                                                change(childWithNullablePk))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition schema")
                .hasMessageContaining("columns");
    }

    @Test
    void rejectsChildrenWithMissingColumn() {
        Table childWithDroppedColumn =
                Table.editor()
                        .tableId(CHILD_2025)
                        .addColumn(column("id", "integer", Types.INTEGER, 1, false))
                        .setPrimaryKeyNames("id")
                        .create();

        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT,
                                        Arrays.asList(
                                                change(ordersTable(CHILD_2024)),
                                                change(childWithDroppedColumn))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition schema")
                .hasMessageContaining("columns");
    }

    @Test
    void rejectsChildrenWithAddedColumnOnlyOnOneChild() {
        Table childWithAddedColumn =
                ordersTable(CHILD_2025)
                        .edit()
                        .addColumn(column("region", "text", Types.VARCHAR, 3, true))
                        .create();

        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT,
                                        Arrays.asList(
                                                change(ordersTable(CHILD_2024)),
                                                change(childWithAddedColumn))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible partition schema")
                .hasMessageContaining("columns");
    }

    @Test
    void propagatesNullableAddedColumnWhenAllChildrenAgree() {
        Table synthesized =
                PartitionSchemaProjector.synthesizeFromChildren(
                        PARENT,
                        Arrays.asList(
                                change(ordersWithRegion(CHILD_2024)),
                                change(ordersWithRegion(CHILD_2025))));

        assertThat(synthesized.id()).isEqualTo(PARENT);
        assertThat(synthesized.retrieveColumnNames()).containsExactly("id", "name", "region");
        assertThat(synthesized.columnWithName("region").isOptional()).isTrue();
    }

    @Test
    void rejectsEmptyChildSchemas() {
        assertThatThrownBy(
                        () ->
                                PartitionSchemaProjector.synthesizeFromChildren(
                                        PARENT, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without child schemas");
    }

    private static Table ordersTable(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(column("id", "integer", Types.INTEGER, 1, false))
                .addColumn(column("name", "text", Types.VARCHAR, 2, true))
                .setPrimaryKeyNames("id")
                .create();
    }

    private static Table ordersWithRegion(TableId tableId) {
        return ordersTable(tableId)
                .edit()
                .addColumn(column("region", "text", Types.VARCHAR, 3, true))
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
