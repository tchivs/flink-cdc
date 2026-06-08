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

import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Unit tests for {@link PartitionReconciler}. */
class PartitionReconcilerTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId KNOWN_CHILD = new TableId(null, "public", "orders_2024");
    private static final TableId NEW_CHILD = new TableId(null, "public", "orders_2025");
    private static final TableId MONITORED_REGULAR_TABLE = new TableId(null, "public", "users");
    private static final TableId UNRELATED = new TableId(null, "public", "audit_log");

    private static PartitionCaptureState routingState() {
        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(PARENT, Collections.singletonList(KNOWN_CHILD));
        return PartitionCaptureState.of(parentToChildren, true, "test_pub");
    }

    @Test
    void onRelationSeenIgnoresKnownParent() {
        PartitionReconciler reconciler = new PartitionReconciler(routingState());

        reconciler.onRelationSeen(PARENT);

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();
    }

    @Test
    void onRelationSeenIgnoresKnownChild() {
        PartitionReconciler reconciler = new PartitionReconciler(routingState());

        reconciler.onRelationSeen(KNOWN_CHILD);

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();
    }

    @Test
    void onRelationSeenIgnoresMonitoredRegularTable() {
        PartitionReconciler reconciler =
                new PartitionReconciler(
                        routingState(), Collections.singleton(MONITORED_REGULAR_TABLE));

        reconciler.onRelationSeen(MONITORED_REGULAR_TABLE);

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();
    }

    @Test
    void onRelationSeenSuppressesIgnoredRelationOnce() {
        Set<TableId> ignoredRelations = ConcurrentHashMap.newKeySet();
        ignoredRelations.add(UNRELATED);
        PartitionReconciler reconciler =
                new PartitionReconciler(routingState(), Collections.emptySet(), ignoredRelations);

        reconciler.onRelationSeen(UNRELATED);

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();

        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(UNRELATED))
                .isInstanceOf(PartitionReconciliationRequiredException.class);
    }

    @Test
    void onRelationSeenFlagsUnknownTableForReconciliation() {
        PartitionReconciler reconciler = new PartitionReconciler(routingState());

        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(NEW_CHILD))
                .isInstanceOf(PartitionReconciliationRequiredException.class);

        Assertions.assertThat(reconciler.needsReconciliation()).isTrue();
        Assertions.assertThat(reconciler.getUnknownRelations()).containsExactly(NEW_CHILD);
    }

    @Test
    void onRelationSeenAccumulatesMultipleUnknowns() {
        PartitionReconciler reconciler = new PartitionReconciler(routingState());

        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(NEW_CHILD))
                .isInstanceOf(PartitionReconciliationRequiredException.class);
        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(UNRELATED))
                .isInstanceOf(PartitionReconciliationRequiredException.class);
        // Duplicate add must not double-count.
        reconciler.onRelationSeen(NEW_CHILD);

        Assertions.assertThat(reconciler.needsReconciliation()).isTrue();
        Assertions.assertThat(reconciler.getUnknownRelations())
                .containsExactlyInAnyOrder(NEW_CHILD, UNRELATED);
    }

    @Test
    void onRelationSeenIsNoOpWhenRoutingDisabled() {
        Map<TableId, List<TableId>> empty = new LinkedHashMap<>();
        PartitionCaptureState disabled = PartitionCaptureState.of(empty, false, null);
        PartitionReconciler reconciler = new PartitionReconciler(disabled);

        reconciler.onRelationSeen(NEW_CHILD);

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();
    }

    @Test
    void resetClearsFlagAndUnknownRelations() {
        PartitionReconciler reconciler = new PartitionReconciler(routingState());
        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(NEW_CHILD))
                .isInstanceOf(PartitionReconciliationRequiredException.class);
        Assertions.assertThatThrownBy(() -> reconciler.onRelationSeen(UNRELATED))
                .isInstanceOf(PartitionReconciliationRequiredException.class);

        reconciler.reset();

        Assertions.assertThat(reconciler.needsReconciliation()).isFalse();
        Assertions.assertThat(reconciler.getUnknownRelations()).isEmpty();
    }
}
