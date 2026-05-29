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

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link PartitionFetchContextCoordinator#deriveParentTables}. */
class PartitionFetchContextCoordinatorTest {

    private static final TableId PARENT_A = new TableId(null, "public", "orders");
    private static final TableId PARENT_B = new TableId(null, "public", "events");
    private static final TableId CHILD_A1 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_A2 = new TableId(null, "public", "orders_2025");

    @Test
    void deriveParentTablesIncludesAllSchemasWhenStateIsEmpty() {
        StreamSplit split = streamSplitWith(PARENT_A, PARENT_B);

        List<TableId> parents =
                PartitionFetchContextCoordinator.deriveParentTables(
                        split, PartitionCaptureState.EMPTY);

        Assertions.assertThat(parents).containsExactlyInAnyOrder(PARENT_A, PARENT_B);
    }

    @Test
    void deriveParentTablesExcludesKnownChildren() {
        StreamSplit split = streamSplitWith(PARENT_A, CHILD_A1, CHILD_A2);
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2));
        PartitionCaptureState state = PartitionCaptureState.of(parentToChildren, true, "test_pub");

        List<TableId> parents = PartitionFetchContextCoordinator.deriveParentTables(split, state);

        // Only PARENT_A is unknown-as-child; children are filtered out.
        Assertions.assertThat(parents).containsExactly(PARENT_A);
    }

    @Test
    void deriveParentTablesFallsBackToCapturedParentsWhenAllSchemasAreKnownChildren() {
        // Split only contains child partitions (the user configured tables.list to children).
        StreamSplit split = streamSplitWith(CHILD_A1, CHILD_A2);
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2));
        PartitionCaptureState state = PartitionCaptureState.of(parentToChildren, true, "test_pub");

        List<TableId> parents = PartitionFetchContextCoordinator.deriveParentTables(split, state);

        // No unknown table in split → fall back to known parents in captureState.
        Assertions.assertThat(parents).containsExactly(PARENT_A);
    }

    @Test
    void deriveParentTablesHandlesNullCaptureState() {
        StreamSplit split = streamSplitWith(PARENT_A, PARENT_B);

        List<TableId> parents = PartitionFetchContextCoordinator.deriveParentTables(split, null);

        // Null state → no known children → all schemas treated as parents.
        Assertions.assertThat(parents).containsExactlyInAnyOrder(PARENT_A, PARENT_B);
    }

    @Test
    void deriveParentTablesReturnsEmptyForEmptySchemasAndEmptyState() {
        StreamSplit split = streamSplitWith();

        List<TableId> parents =
                PartitionFetchContextCoordinator.deriveParentTables(
                        split, PartitionCaptureState.EMPTY);

        Assertions.assertThat(parents).isEmpty();
    }

    private static StreamSplit streamSplitWith(TableId... tableIds) {
        Map<TableId, TableChange> schemas = new LinkedHashMap<>();
        for (TableId id : tableIds) {
            // deriveParentTables only inspects keySet(), so null TableChange is fine.
            schemas.put(id, null);
        }
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID, null, null, Collections.emptyList(), schemas, 0);
    }
}
