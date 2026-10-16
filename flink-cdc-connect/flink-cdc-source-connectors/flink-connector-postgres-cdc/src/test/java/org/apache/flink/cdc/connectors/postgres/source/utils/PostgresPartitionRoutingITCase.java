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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Partition routing and {@link PartitionGuard} against a real PostgreSQL 10 catalog, where {@code
 * pg_inherits} mixes declarative partitions with legacy {@code INHERITS} children.
 */
class PostgresPartitionRoutingITCase {

    private static final PostgreSQLContainer<?> PG10 =
            new PostgreSQLContainer<>(
                            DockerImageName.parse("debezium/postgres:10")
                                    .asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("routing")
                    .withUsername("postgres")
                    .withPassword("postgres");

    private static final List<String> CONFIGURED =
            Arrays.asList("public.orders", "public.events", "public.legacy");

    @BeforeAll
    static void createTables() throws Exception {
        PG10.start();
        try (Connection connection =
                        DriverManager.getConnection(
                                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword());
                Statement statement = connection.createStatement()) {
            // single level declarative partitioning
            statement.execute(
                    "CREATE TABLE public.orders (id INT, dt DATE) PARTITION BY RANGE (dt)");
            statement.execute(
                    "CREATE TABLE public.orders_202610 PARTITION OF public.orders"
                            + " FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')");
            // two level declarative partitioning
            statement.execute(
                    "CREATE TABLE public.events (id INT, dt DATE, kind INT)"
                            + " PARTITION BY RANGE (dt)");
            statement.execute(
                    "CREATE TABLE public.events_2026 PARTITION OF public.events"
                            + " FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
                            + " PARTITION BY LIST (kind)");
            statement.execute(
                    "CREATE TABLE public.events_2026_k1 PARTITION OF public.events_2026"
                            + " FOR VALUES IN (1)");
            // legacy inheritance: the parent stores rows of its own
            statement.execute("CREATE TABLE public.legacy (id INT PRIMARY KEY)");
            statement.execute("CREATE TABLE public.legacy_child () INHERITS (public.legacy)");
            // a partition attached after the routing was resolved
            statement.execute("CREATE TABLE public.orders_202611 (id INT, dt DATE)");
            statement.execute("CREATE TABLE public.unrelated (id INT)");
        }
    }

    @AfterAll
    static void stopContainer() {
        PG10.stop();
    }

    @Test
    void testLegacyInheritanceIsNeitherRoutedNorDroppedFromSnapshot() {
        PostgresPartitionRouting routing = resolve();

        assertThat(routing.parentOf("public.orders_202610")).contains("public.orders");
        assertThat(routing.parentOf("public.events_2026_k1")).contains("public.events");
        assertThat(routing.hasCapturedPartitions("public.events_2026")).isTrue();
        assertThat(routing.parentOf("public.legacy_child")).isEmpty();
        assertThat(routing.hasCapturedPartitions("public.legacy")).isFalse();
    }

    @Test
    void testGuardFailsForPartitionAttachedAfterResolution() throws Exception {
        PartitionGuard guard = guard();
        try (Connection connection =
                        DriverManager.getConnection(
                                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "ALTER TABLE public.orders ATTACH PARTITION public.orders_202611"
                            + " FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')");
        }

        assertThatThrownBy(() -> guard.check(new TableId(null, "public", "orders_202611")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("public.orders_202611")
                .hasMessageContaining("public.orders");
        assertThatThrownBy(() -> guard.check(new TableId(null, "public", "events_2026_k1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("partition root public.events");
    }

    @Test
    void testGuardPassesTablesThatAreNoConfiguredPartition() {
        PartitionGuard guard = guard();

        assertThatCode(() -> guard.check(new TableId(null, "public", "unrelated")))
                .doesNotThrowAnyException();
        assertThatCode(() -> guard.check(new TableId(null, "public", "legacy_child")))
                .doesNotThrowAnyException();
        PartitionGuard ordersOnly =
                new PartitionGuard(
                        PG10.getJdbcUrl(),
                        PG10.getUsername(),
                        PG10.getPassword(),
                        Collections.singletonList("public.orders"));
        assertThatCode(() -> ordersOnly.check(new TableId(null, "public", "events_2026_k1")))
                .doesNotThrowAnyException();
    }

    private static PostgresPartitionRouting resolve() {
        return PostgresPartitionRouting.resolve(
                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword(), CONFIGURED, true);
    }

    private static PartitionGuard guard() {
        return new PartitionGuard(
                PG10.getJdbcUrl(), PG10.getUsername(), PG10.getPassword(), CONFIGURED);
    }
}
