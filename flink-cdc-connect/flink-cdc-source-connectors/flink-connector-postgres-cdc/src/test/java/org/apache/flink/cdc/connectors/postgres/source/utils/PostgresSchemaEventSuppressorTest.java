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
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSchemaEventSuppressor}. */
class PostgresSchemaEventSuppressorTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");

    @Test
    void suppressesDuplicateChildSchemaEventsForRoutedParent() {
        PostgresSchemaEventSuppressor suppressor = new PostgresSchemaEventSuppressor();

        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2024, "integer"), table(PARENT, "integer")))
                .isTrue();
        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2024, "integer"), table(PARENT, "integer")))
                .isFalse();
        assertThat(suppressor.shouldDispatch(table(PARENT, "integer"), table(PARENT, "integer")))
                .isTrue();
    }

    @Test
    void doesNotCompareIdentityRoutedTablesAgainstPartitionFingerprints() {
        PostgresSchemaEventSuppressor suppressor = new PostgresSchemaEventSuppressor();

        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2024, "integer"), table(PARENT, "integer")))
                .isTrue();
        assertThat(suppressor.shouldDispatch(table(PARENT, "bigint"), table(PARENT, "bigint")))
                .isTrue();
    }

    @Test
    void tracksFingerprintsIndependentlyPerRoutedParentAndChild() {
        PostgresSchemaEventSuppressor suppressor = new PostgresSchemaEventSuppressor();

        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2024, "integer"), table(PARENT, "integer")))
                .isTrue();
        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2025, "integer"), table(PARENT, "integer")))
                .isTrue();
        assertThat(
                        suppressor.shouldDispatch(
                                table(CHILD_2025, "integer"), table(PARENT, "integer")))
                .isFalse();
        assertThat(suppressor.shouldDispatch(table(CHILD_2025, "bigint"), table(PARENT, "bigint")))
                .isTrue();
    }

    private static Table table(TableId tableId, String idType) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(column("id", idType, 1, false))
                .addColumn(column("name", "text", 2, true))
                .setPrimaryKeyNames("id")
                .create();
    }

    private static Column column(String name, String type, int position, boolean optional) {
        return Column.editor()
                .name(name)
                .type(type)
                .jdbcType(jdbcType(type))
                .position(position)
                .optional(optional)
                .create();
    }

    private static int jdbcType(String type) {
        if ("bigint".equals(type)) {
            return Types.BIGINT;
        }
        if ("integer".equals(type)) {
            return Types.INTEGER;
        }
        return Types.VARCHAR;
    }
}
