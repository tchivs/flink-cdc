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

import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the partition-routing contract that {@link PostgresDialect} exposes to {@link
 * org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext#getTableId}.
 *
 * <p>{@code getTableId} routes child→parent purely via {@code mapping.get(tableId)} where {@code
 * mapping} comes from {@link PostgresDialect#getChildToParentMapping()}. These tests pin down the
 * mapping behaviour so a regression in the routing source-of-truth is caught at the unit level.
 */
class PostgresDialectPartitionRoutingTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_1 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2 = new TableId(null, "public", "orders_2025");
    private static final TableId UNRELATED = new TableId(null, "public", "users");

    @Test
    void getChildToParentMappingIsEmptyByDefault() {
        PostgresDialect dialect = new PostgresDialect(null);

        Assertions.assertThat(dialect.getChildToParentMapping()).isEmpty();
    }

    @Test
    void getChildToParentMappingReflectsAppliedCaptureState() {
        PostgresDialect dialect = new PostgresDialect(null);
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(PARENT, Arrays.asList(CHILD_1, CHILD_2));
        PartitionCaptureState state = PartitionCaptureState.of(parentToChildren, true, "test_pub");

        dialect.setCurrentPartitionState(state);

        Map<TableId, TableId> mapping = dialect.getChildToParentMapping();
        Assertions.assertThat(mapping)
                .containsEntry(CHILD_1, PARENT)
                .containsEntry(CHILD_2, PARENT);
        // getTableId returns the routed parent for known children.
        Assertions.assertThat(mapping.get(CHILD_1)).isEqualTo(PARENT);
        Assertions.assertThat(mapping.get(CHILD_2)).isEqualTo(PARENT);
        // Unrelated table is not in mapping → getTableId returns the original tableId unchanged.
        Assertions.assertThat(mapping.get(UNRELATED)).isNull();
    }

    @Test
    void setCurrentPartitionStateNullFallsBackToEmpty() {
        PostgresDialect dialect = new PostgresDialect(null);
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(PARENT, Collections.singletonList(CHILD_1));
        dialect.setCurrentPartitionState(
                PartitionCaptureState.of(parentToChildren, true, "test_pub"));

        dialect.setCurrentPartitionState(null);

        Assertions.assertThat(dialect.getChildToParentMapping()).isEmpty();
    }

    @Test
    void setCurrentPartitionStateOverwritesPreviousMapping() {
        PostgresDialect dialect = new PostgresDialect(null);

        Map<TableId, List<TableId>> first = new LinkedHashMap<>();
        first.put(PARENT, Collections.singletonList(CHILD_1));
        dialect.setCurrentPartitionState(PartitionCaptureState.of(first, true, "test_pub"));

        Map<TableId, List<TableId>> second = new LinkedHashMap<>();
        second.put(PARENT, Arrays.asList(CHILD_1, CHILD_2));
        dialect.setCurrentPartitionState(PartitionCaptureState.of(second, true, "test_pub"));

        Map<TableId, TableId> mapping = dialect.getChildToParentMapping();
        Assertions.assertThat(mapping).containsKeys(CHILD_1, CHILD_2);
    }
}
