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

import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionTopology.Marker;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PartitionTopology}. */
class PartitionTopologyTest {

    private static final String DATABASE = "db";
    private static final List<String> CONFIGURED = Collections.singletonList("public.m");

    private static final Marker BASELINE = marker(10, "public.m", "public.m_1", "public.m_2");
    private static final Marker DETACH_M1 = marker(20, "public.m", "public.m_2");
    private static final Marker ATTACH_M3 = marker(30, "public.m", "public.m_2", "public.m_3");
    private static final List<Marker> HISTORY = Arrays.asList(BASELINE, DETACH_M1, ATTACH_M3);

    /** Today's catalog: m_1 detached, m_3 attached. */
    private static Map<String, List<String>> catalog() {
        Map<String, List<String>> catalog = new HashMap<>();
        catalog.put("public.m", Arrays.asList("public.m_2", "public.m_3"));
        return catalog;
    }

    @Test
    void startPositionTakesTheMarkersNotAfterIt() {
        PartitionTopology beforeDetach = at(19);
        assertThat(beforeDetach.routing().routedPartitions())
                .containsExactlyInAnyOrder("public.m_1", "public.m_2");
        assertThat(beforeDetach.isUnrouted(table("m_1"))).isFalse();

        // A marker exactly at the start position is applied: its DDL held the partition locked.
        assertThat(at(20).routing().routedPartitions()).containsExactly("public.m_2");
        assertThat(at(30).routing().routedPartitions())
                .containsExactlyInAnyOrder("public.m_2", "public.m_3");
    }

    @Test
    void positionBeforeTheMarkerTableFallsBackToTheCatalog() {
        assertThat(at(5).routing().routedPartitions())
                .containsExactlyInAnyOrder("public.m_2", "public.m_3");
        assertThat(
                        PartitionTopology.atPosition(
                                        catalog(),
                                        Collections.emptyList(),
                                        100,
                                        DATABASE,
                                        CONFIGURED)
                                .routing()
                                .routedPartitions())
                .containsExactlyInAnyOrder("public.m_2", "public.m_3");
    }

    @Test
    void markersFromTheStreamMoveTheRoutingOnceEach() {
        PartitionTopology topology = at(19);
        // Replayed stale marker: already part of the start state.
        assertThat(topology.apply(BASELINE)).isFalse();

        assertThat(topology.apply(DETACH_M1)).isTrue();
        assertThat(topology.isUnrouted(table("m_1"))).isTrue();
        assertThat(topology.apply(DETACH_M1)).isFalse();

        // m_3 was never routed before its ATTACH: not "unrouted", but captured once routed.
        assertThat(topology.isUnrouted(table("m_3"))).isFalse();
        assertThat(topology.apply(ATTACH_M3)).isTrue();
        assertThat(topology.routing().parentOf("public.m_3")).contains("public.m");
    }

    @Test
    void routedPartitionOutsideTheIncludeListFails() {
        PartitionTopology topology = at(30);
        assertThatThrownBy(() -> topology.checkFiltered(table("m_3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("public.m_3");
        topology.checkFiltered(table("other"));
    }

    @Test
    void renamedMovedAndDroppedPartitionsLeaveTheirFormerPlace() {
        PartitionTopology topology = at(10);
        // RENAME m_2 TO m_2b: the parent's row lists the new name only.
        topology.apply(marker(11, "public.m", "public.m_1", "public.m_2b"));
        assertThat(topology.routing().routedPartitions())
                .containsExactlyInAnyOrder("public.m_1", "public.m_2b");
        // m_1 moved under an unconfigured root: the row of the new parent takes it.
        topology.apply(marker(12, "public.archive", "public.m_1"));
        assertThat(topology.routing().routedPartitions()).containsExactly("public.m_2b");
        // DROP TABLE m_2b.
        topology.apply(new Marker(13, "public.m_2b", null));
        assertThat(topology.routing().isEmpty()).isTrue();
    }

    @Test
    void subPartitionsAreRoutedToTheTopLevelRoot() {
        PartitionTopology topology = at(10);
        topology.apply(marker(11, "public.m_2", "public.m_2_a"));
        assertThat(topology.routing().parentOf("public.m_2_a")).contains("public.m");
        assertThat(topology.routing().hasCapturedPartitions("public.m_2")).isTrue();
        // DETACH the intermediate level: its leaves leave the root with it.
        topology.apply(marker(12, "public.m", "public.m_1"));
        assertThat(topology.isUnrouted(table("m_2_a"))).isTrue();
    }

    @Test
    void everyPartitionEverRoutedIsInTheIncludeList() {
        assertThat(PartitionTopology.partitionsEverRouted(catalog(), HISTORY, DATABASE, CONFIGURED))
                .containsExactly("public.m_1", "public.m_2", "public.m_3");
    }

    private static PartitionTopology at(long position) {
        return PartitionTopology.atPosition(catalog(), HISTORY, position, DATABASE, CONFIGURED);
    }

    private static Marker marker(long lsn, String parent, String... children) {
        return new Marker(lsn, parent, Arrays.asList(children));
    }

    private static TableId table(String name) {
        return new TableId(null, "public", name);
    }
}
