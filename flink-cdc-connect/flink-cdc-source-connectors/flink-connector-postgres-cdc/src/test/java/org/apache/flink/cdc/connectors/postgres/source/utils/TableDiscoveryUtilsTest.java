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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableDiscoveryUtils}. */
class TableDiscoveryUtilsTest {

    private static final String DATABASE = "aiadb";
    private static final String SCHEMA = "public";
    private static final String PARENT = SCHEMA + ".aia_t_icc_jjdb";
    private static final String PARTITION = SCHEMA + ".aia_t_icc_jjdb_202609";

    @Test
    void testPartitionRootIsDroppedAndLeafPartitionIsKept() {
        // Regression: on a server without publish_via_partition_root the root holds no rows of its
        // own and cannot carry a primary key on PostgreSQL 10, so leaving it in the discovered list
        // fails the snapshot chunk splitter and reads every row twice.
        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        Collections.singletonMap(PARTITION, Collections.singleton(PARENT)),
                        DATABASE,
                        Collections.singletonList(PARENT));

        assertThat(routing.isEmpty()).isFalse();
        List<TableId> discovered =
                Arrays.asList(
                        tableId("aia_t_icc_jjdb"),
                        tableId("aia_t_icc_jjdb_202609"),
                        tableId("aia_t_vcs_zjdb"));

        assertThat(TableDiscoveryUtils.withoutPartitionRoots(discovered, routing))
                .extracting(TableId::table)
                .containsExactly("aia_t_icc_jjdb_202609", "aia_t_vcs_zjdb");
    }

    @Test
    void testIntermediatePartitionLevelIsDroppedToo() {
        Map<String, Set<String>> ancestors = new LinkedHashMap<>();
        ancestors.put("public.b", Collections.singleton("public.a"));
        ancestors.put("public.c", new LinkedHashSet<>(Arrays.asList("public.a", "public.b")));
        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        ancestors, DATABASE, Collections.singletonList("public.a"));

        List<TableId> discovered = Arrays.asList(tableId("a"), tableId("b"), tableId("c"));

        assertThat(TableDiscoveryUtils.withoutPartitionRoots(discovered, routing))
                .extracting(TableId::table)
                .containsExactly("c");
    }

    @Test
    void testPartitionQueryOfAnUnconfiguredFamilyIsUntouched() {
        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        Collections.singletonMap(PARTITION, Collections.singleton(PARENT)),
                        DATABASE,
                        Collections.singletonList("public.other_table"));

        assertThat(routing.isEmpty()).isTrue();
        List<TableId> discovered =
                Arrays.asList(tableId("aia_t_icc_jjdb"), tableId("aia_t_vcs_zjdb"));

        assertThat(TableDiscoveryUtils.withoutPartitionRoots(discovered, routing))
                .extracting(TableId::table)
                .containsExactly("aia_t_icc_jjdb", "aia_t_vcs_zjdb");
    }

    @Test
    void testDiscoveryIsUntouchedWithoutRouting() {
        List<TableId> discovered =
                Arrays.asList(tableId("aia_t_icc_jjdb"), tableId("aia_t_vcs_zjdb"));

        assertThat(
                        TableDiscoveryUtils.withoutPartitionRoots(
                                discovered, PostgresPartitionRouting.empty()))
                .isSameAs(discovered);
    }

    private static TableId tableId(String table) {
        return new TableId(DATABASE, SCHEMA, table);
    }
}
