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

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link PartitionCaptureState} immutable snapshot behavior. */
class PartitionCaptureStateTest {

    private static final TableId PARENT_A = new TableId(null, "public", "products");
    private static final TableId CHILD_A1 = new TableId(null, "public", "products_us");
    private static final TableId CHILD_A2 = new TableId(null, "public", "products_uk");
    private static final TableId PARENT_B = new TableId(null, "public", "orders");
    private static final TableId CHILD_B1 = new TableId(null, "public", "orders_2024");

    @Test
    void emptyStateIsRoutingDisabled() {
        Assertions.assertThat(PartitionCaptureState.EMPTY.isRoutingEnabled()).isFalse();
        Assertions.assertThat(PartitionCaptureState.EMPTY.getChildToParent()).isEmpty();
        Assertions.assertThat(PartitionCaptureState.EMPTY.getParentToChildren()).isEmpty();
        Assertions.assertThat(PartitionCaptureState.EMPTY.getPublicationName()).isNull();
    }

    @Test
    void factoryDerivesChildToParentMapping() {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        parentToChildren.put(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2));
        parentToChildren.put(PARENT_B, Collections.singletonList(CHILD_B1));

        PartitionCaptureState state =
                PartitionCaptureState.of(parentToChildren, true, "dbz_publication");

        Assertions.assertThat(state.isRoutingEnabled()).isTrue();
        Assertions.assertThat(state.getPublicationName()).isEqualTo("dbz_publication");
        Assertions.assertThat(state.getChildToParent())
                .hasSize(3)
                .containsEntry(CHILD_A1, PARENT_A)
                .containsEntry(CHILD_A2, PARENT_A)
                .containsEntry(CHILD_B1, PARENT_B);
    }

    @Test
    void getParentForReturnsNullForUnknownChild() {
        PartitionCaptureState state =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2)),
                        true,
                        null);

        Assertions.assertThat(state.getParentFor(CHILD_A1)).isEqualTo(PARENT_A);
        Assertions.assertThat(state.getParentFor(new TableId(null, "public", "unknown"))).isNull();
    }

    @Test
    void isChildPartitionAndIsParentTable() {
        PartitionCaptureState state =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        null);

        Assertions.assertThat(state.isParentTable(PARENT_A)).isTrue();
        Assertions.assertThat(state.isChildPartition(CHILD_A1)).isTrue();
        Assertions.assertThat(state.isChildPartition(PARENT_A)).isFalse();
        Assertions.assertThat(state.isParentTable(CHILD_A1)).isFalse();
    }

    @Test
    void withUpdatedMappingsAddsNewParents() {
        PartitionCaptureState initial =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        "p");

        Map<TableId, List<TableId>> add = new HashMap<>();
        add.put(PARENT_B, Collections.singletonList(CHILD_B1));
        PartitionCaptureState updated = initial.withUpdatedMappings(add);

        Assertions.assertThat(updated.getChildToParent())
                .containsEntry(CHILD_A1, PARENT_A)
                .containsEntry(CHILD_B1, PARENT_B);
        Assertions.assertThat(updated.getPublicationName()).isEqualTo("p");
        Assertions.assertThat(updated.isRoutingEnabled()).isTrue();
        // Original is untouched
        Assertions.assertThat(initial.getChildToParent()).hasSize(1);
    }

    @Test
    void withUpdatedMappingsOverwritesExistingParent() {
        PartitionCaptureState initial =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        null);

        Map<TableId, List<TableId>> overwrite = new HashMap<>();
        overwrite.put(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2));
        PartitionCaptureState updated = initial.withUpdatedMappings(overwrite);

        Assertions.assertThat(updated.getChildToParent())
                .hasSize(2)
                .containsEntry(CHILD_A1, PARENT_A)
                .containsEntry(CHILD_A2, PARENT_A);
    }

    @Test
    void mapsAreUnmodifiableFromOutside() {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        parentToChildren.put(PARENT_A, Collections.singletonList(CHILD_A1));
        PartitionCaptureState state = PartitionCaptureState.of(parentToChildren, true, null);

        Assertions.assertThatThrownBy(() -> state.getChildToParent().put(CHILD_A2, PARENT_A))
                .isInstanceOf(UnsupportedOperationException.class);
        Assertions.assertThatThrownBy(
                        () ->
                                state.getParentToChildren()
                                        .put(PARENT_B, Collections.singletonList(CHILD_B1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void defensiveCopyOfInputMap() {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        parentToChildren.put(PARENT_A, Collections.singletonList(CHILD_A1));
        PartitionCaptureState state = PartitionCaptureState.of(parentToChildren, true, null);

        // Mutate the original map after construction; state must not observe the mutation.
        parentToChildren.put(PARENT_B, Collections.singletonList(CHILD_B1));

        Assertions.assertThat(state.getParentToChildren()).hasSize(1).containsKey(PARENT_A);
        Assertions.assertThat(state.getChildToParent()).hasSize(1).containsKey(CHILD_A1);
    }

    @Test
    void equalsAndHashCodeBasedOnParentMapAndPublication() {
        PartitionCaptureState a =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        "p");
        PartitionCaptureState b =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        "p");
        PartitionCaptureState differentPub =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Collections.singletonList(CHILD_A1)),
                        true,
                        "other");

        Assertions.assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        Assertions.assertThat(a).isNotEqualTo(differentPub);
    }

    @Test
    void toStringIsHumanReadable() {
        PartitionCaptureState state =
                PartitionCaptureState.of(
                        Collections.singletonMap(PARENT_A, Arrays.asList(CHILD_A1, CHILD_A2)),
                        true,
                        "dbz_publication");
        Assertions.assertThat(state.toString())
                .contains("routing=true")
                .contains("parents=1")
                .contains("children=2")
                .contains("dbz_publication");
    }
}
