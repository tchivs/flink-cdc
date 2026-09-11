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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresPartitionRouting}. */
class PostgresPartitionRoutingTest {

    private static final String DATABASE = "aiadb";
    private static final String PARENT = "public.aia_t_icc_jjdb";
    private static final String PARTITION = "public.aia_t_icc_jjdb_202609";

    @Test
    void testPartitionIsRoutedToConfiguredParent() {
        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        ancestors(PARTITION, PARENT),
                        DATABASE,
                        Collections.singletonList(PARENT));

        assertThat(routing.isEmpty()).isFalse();
        assertThat(routing.parentOf(PARTITION)).contains(PARENT);
        assertThat(routing.parentOf(PARENT)).isEmpty();
        assertThat(routing.parentOf("public.aia_t_icc_jjdb_202610")).isEmpty();
        assertThat(routing.sampleChildOf(PARENT)).contains(PARTITION);
        assertThat(routing.sampleChildOf("public.aia_t_icc_jjdb_202610")).isEmpty();
        assertThat(routing.childrenOf(PARENT)).containsExactly(PARTITION);
        assertThat(
                        routing.expansionForDebeziumIncludeList(
                                DATABASE, Collections.singletonList(PARENT)))
                .containsExactly(PARTITION, DATABASE + "." + PARTITION);
    }

    @Test
    void testExpansionCarriesBothShapesForDatabaseQualifiedParents() {
        // The regression this guards: with a database-qualified configured parent the expansion
        // used to emit only the qualified child, so the replication connection (which matches
        // schema.table) silently dropped every partition change.
        Map<String, Set<String>> ancestors = ancestors(PARTITION, PARENT);

        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        ancestors, DATABASE, Collections.singletonList(DATABASE + "." + PARENT));

        assertThat(routing.isEmpty()).isFalse();
        assertThat(routing.parentOf(PARTITION)).contains(PARENT);
        assertThat(
                        routing.expansionForDebeziumIncludeList(
                                DATABASE, Collections.singletonList(DATABASE + "." + PARENT)))
                .containsExactly(PARTITION, DATABASE + "." + PARTITION);
    }

    @Test
    void testParentMayBeConfiguredWithOrWithoutDatabase() {
        Map<String, Set<String>> ancestors = ancestors(PARTITION, PARENT);

        assertThat(
                        PostgresPartitionRouting.create(
                                        ancestors, DATABASE, Collections.singletonList(PARENT))
                                .isEmpty())
                .isFalse();
        assertThat(
                        PostgresPartitionRouting.create(
                                        ancestors,
                                        DATABASE,
                                        Collections.singletonList(DATABASE + "." + PARENT))
                                .isEmpty())
                .isFalse();
    }

    @Test
    void testUnconfiguredParentIsNotRouted() {
        Map<String, Set<String>> ancestors = ancestors(PARTITION, PARENT);

        // a list that names the partitions themselves, and a list that names another database,
        // must not turn the partitions into children of a configured table
        assertThat(
                        PostgresPartitionRouting.create(
                                        ancestors,
                                        DATABASE,
                                        Collections.singletonList(
                                                "public.aia_t_icc_jjdb_\\d{6}"))
                                .isEmpty())
                .isTrue();
        assertThat(
                        PostgresPartitionRouting.create(
                                        ancestors,
                                        DATABASE,
                                        Collections.singletonList("otherdb." + PARENT))
                                .isEmpty())
                .isTrue();
    }

    @Test
    void testMultiLevelPartitioningIsRoutedToTheRoot() {
        Map<String, Set<String>> ancestors = new LinkedHashMap<>();
        ancestors.put("public.b", Collections.singleton("public.a"));
        ancestors.put("public.c", new LinkedHashSet<>(Arrays.asList("public.a", "public.b")));

        PostgresPartitionRouting routing =
                PostgresPartitionRouting.create(
                        ancestors, DATABASE, Collections.singletonList("public.a"));

        assertThat(routing.parentOf("public.b")).contains("public.a");
        assertThat(routing.parentOf("public.c")).contains("public.a");
        assertThat(routing.childrenOf("public.a")).containsExactly("public.b", "public.c");
    }

    @Test
    void testDisabledFeatureAndUnreadableDatabaseDegradeToEmptyRouting() {
        assertThat(
                        PostgresPartitionRouting.resolve(
                                        "jdbc:postgresql://127.0.0.1:1/" + DATABASE,
                                        "user",
                                        "password",
                                        Collections.singletonList(PARENT),
                                        false)
                                .isEmpty())
                .isTrue();
        assertThat(
                        PostgresPartitionRouting.resolve(
                                        "jdbc:postgresql://127.0.0.1:1/" + DATABASE,
                                        "user",
                                        "password",
                                        Collections.emptyList(),
                                        true)
                                .isEmpty())
                .isTrue();
        // the job must not fail when the server cannot be reached
        assertThat(
                        PostgresPartitionRouting.resolve(
                                        "jdbc:postgresql://127.0.0.1:1/" + DATABASE,
                                        "user",
                                        "password",
                                        Collections.singletonList(PARENT),
                                        true)
                                .isEmpty())
                .isTrue();
    }

    @Test
    void testDatabaseNameOfJdbcUrl() {
        assertThat(PostgresPartitionRouting.databaseOf("jdbc:postgresql://db:5432/" + DATABASE))
                .isEqualTo(DATABASE);
        assertThat(
                        PostgresPartitionRouting.databaseOf(
                                "jdbc:postgresql://db:5432/" + DATABASE + "?sslmode=require"))
                .isEqualTo(DATABASE);
    }

    private static Map<String, Set<String>> ancestors(String child, String parent) {
        Map<String, Set<String>> ancestors = new LinkedHashMap<>();
        ancestors.put(child, Collections.singleton(parent));
        return ancestors;
    }
}
