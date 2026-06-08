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

import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionAwareTableFilter}. */
class PartitionAwareTableFilterTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD = new TableId(null, "public", "orders_2025");
    private static final TableId CHILD_2026 = new TableId(null, "public", "orders_2026");
    private static final TableId OTHER = new TableId(null, "public", "other");

    @Test
    void includesOriginalAndRoutedChildTables() {
        Tables.TableFilter originalFilter = PARENT::equals;
        PartitionRoutingState routingState =
                PartitionRoutingState.of(Collections.singletonMap(PARENT, Arrays.asList(CHILD)));
        PartitionAwareTableFilter filter =
                new PartitionAwareTableFilter(originalFilter, () -> routingState);

        assertThat(filter.isIncluded(PARENT)).isTrue();
        assertThat(filter.isIncluded(CHILD)).isTrue();
        assertThat(filter.isIncluded(OTHER)).isFalse();
    }

    @Test
    void failsWhenRoutingStateIsMissing() {
        PartitionAwareTableFilter filter =
                new PartitionAwareTableFilter(tableId -> false, () -> null);

        assertThatThrownBy(() -> filter.isIncluded(CHILD))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Partition routing state is not initialized");
    }

    @Test
    void wrapsPostgresConnectorConfigTableFilters() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        PartitionRoutingState routingState =
                PartitionRoutingState.of(Collections.singletonMap(PARENT, Arrays.asList(CHILD)));

        PostgresConnectorConfig wrappedConfig =
                PartitionAwarePostgresConnectorConfig.wrap(config, () -> routingState);

        assertThat(wrappedConfig.getTableFilters().dataCollectionFilter())
                .isInstanceOf(PartitionAwareTableFilter.class);
        assertThat(wrappedConfig.getTableFilters().dataCollectionFilter().isIncluded(PARENT))
                .isTrue();
        assertThat(wrappedConfig.getTableFilters().dataCollectionFilter().isIncluded(CHILD))
                .isTrue();
        assertThat(wrappedConfig.getTableFilters().dataCollectionFilter().isIncluded(OTHER))
                .isFalse();
        assertThat(wrappedConfig.getTableFilters().eligibleDataCollectionFilter().isIncluded(CHILD))
                .isEqualTo(
                        config.getTableFilters().eligibleDataCollectionFilter().isIncluded(CHILD));
    }

    @Test
    void resolvesFilteredPublicationMembersToChildrenOnlyForOriginalParentMatch() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD, CHILD_2026)));
        PartitionAwareTableFilter filter =
                new PartitionAwareTableFilter(PARENT::equals, () -> routingState);
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(PARENT, CHILD, CHILD_2026));

        assertThat(filter.resolvePublicationMembers(capturedTables))
                .containsExactly(CHILD, CHILD_2026);
    }

    @Test
    void rejectsFilteredPublicationMembersWhenOriginalFilterMatchesParentAndChild() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD, CHILD_2026)));
        PartitionAwareTableFilter filter =
                new PartitionAwareTableFilter(
                        tableId -> PARENT.equals(tableId) || CHILD.equals(tableId),
                        () -> routingState);
        Set<TableId> capturedTables = new LinkedHashSet<>(Arrays.asList(PARENT, CHILD, CHILD_2026));

        assertThatThrownBy(() -> filter.resolvePublicationMembers(capturedTables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_004);
    }
}
