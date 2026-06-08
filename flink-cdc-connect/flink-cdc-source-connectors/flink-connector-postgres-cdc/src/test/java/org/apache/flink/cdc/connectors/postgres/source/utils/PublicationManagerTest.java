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

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Unit tests for {@link PublicationManager}. */
class PublicationManagerTest {

    private static final TableId PARENT = new TableId("inventory", "public", "orders");
    private static final TableId CHILD_1 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2 = new TableId(null, "public", "orders_2025");
    private static final TableId CATALOG_CHILD_1 =
            new TableId("inventory", "public", "orders_2024");
    private static final TableId CATALOG_CHILD_2 =
            new TableId("inventory", "public", "orders_2025");
    private static final TableId REGULAR_TABLE = new TableId("inventory", "public", "customers");

    @Test
    void findMissingChildrenReturnsEmptyForEmptyInput() throws SQLException {
        // No children means no need to query JDBC, so a null connection is safe here.
        Assertions.assertThat(
                        PublicationManager.findMissingChildren(
                                null, "pub", Collections.emptyList()))
                .isEmpty();
    }

    @Test
    void findMissingChildrenReturnsEmptyForNullInput() throws SQLException {
        Assertions.assertThat(PublicationManager.findMissingChildren(null, "pub", null)).isEmpty();
    }

    @Test
    void addTablesToPublicationIsNoOpForEmptyInput() throws SQLException {
        // Empty input must short-circuit before touching the connection.
        PublicationManager.addTablesToPublication(null, "pub", Collections.emptyList());
        PublicationManager.addTablesToPublication(null, "pub", null);
    }

    @Test
    void validateMembershipIsNoOpForEmptyInput() throws SQLException {
        PublicationManager.validateMembership(null, "pub", Collections.emptyList());
        PublicationManager.validateMembership(null, "pub", null);
    }

    @Test
    void validateMembershipThrowsWhenChildrenMissingFromPublication() {
        // Stub publication contains nothing → all input children are reported as missing.
        JdbcConnection jdbc = stubJdbcConnection(Collections.emptySet());

        List<TableId> children = Arrays.asList(CHILD_1, CHILD_2);

        Assertions.assertThatThrownBy(
                        () -> PublicationManager.validateMembership(jdbc, "test_pub", children))
                .isInstanceOf(PublicationManager.PublicationValidationException.class)
                .hasMessageContaining("test_pub")
                .hasMessageContaining("orders_2024")
                .hasMessageContaining("orders_2025")
                .hasMessageContaining("ALTER PUBLICATION");
    }

    @Test
    void validateMembershipPassesWhenAllChildrenAreInPublication() throws SQLException {
        Set<TableId> published = new HashSet<>(Arrays.asList(CHILD_1, CHILD_2));
        JdbcConnection jdbc = stubJdbcConnection(published);

        // Should not throw.
        PublicationManager.validateMembership(jdbc, "test_pub", Arrays.asList(CHILD_1, CHILD_2));
    }

    @Test
    void findMissingChildrenReportsOnlyAbsentTables() throws SQLException {
        // Only CHILD_1 is published; CHILD_2 should be reported as missing.
        JdbcConnection jdbc = stubJdbcConnection(Collections.singleton(CHILD_1));

        List<TableId> missing =
                PublicationManager.findMissingChildren(
                        jdbc, "test_pub", Arrays.asList(CHILD_1, CHILD_2));

        Assertions.assertThat(missing).containsExactly(CHILD_2);
    }

    @Test
    void resolveFilteredPublicationTablesReplacesParentsWithChildPartitions() throws SQLException {
        JdbcConnection jdbc =
                stubJdbcConnectionWithResults(
                        Arrays.asList(
                                stubRows(
                                        Collections.singletonList(
                                                row(
                                                        "catalog_name",
                                                        "inventory",
                                                        "schema_name",
                                                        "public",
                                                        "table_name",
                                                        "orders"))),
                                stubRows(
                                        Arrays.asList(
                                                row(
                                                        "parent_catalog",
                                                        "inventory",
                                                        "child_schema",
                                                        "public",
                                                        "child_table",
                                                        "orders_2024"),
                                                row(
                                                        "parent_catalog",
                                                        "inventory",
                                                        "child_schema",
                                                        "public",
                                                        "child_table",
                                                        "orders_2025"))),
                                stubRows(
                                        Collections.singletonList(
                                                row(
                                                        "catalog_name",
                                                        "inventory",
                                                        "schema_name",
                                                        "public",
                                                        "table_name",
                                                        "orders")))));

        Set<TableId> publicationTables =
                PublicationManager.resolveFilteredPublicationTables(
                        jdbc, "test_pub", new HashSet<>(Arrays.asList(PARENT, REGULAR_TABLE)));

        Assertions.assertThat(publicationTables)
                .containsExactlyInAnyOrder(REGULAR_TABLE, CATALOG_CHILD_1, CATALOG_CHILD_2)
                .doesNotContain(PARENT);
    }

    /**
     * Builds a {@link JdbcConnection} whose {@link JdbcConnection#connection()} returns a {@link
     * Proxy} stub of {@link Connection}, which in turn answers queries against
     * pg_publication_tables with the given set of tables.
     */
    private static JdbcConnection stubJdbcConnection(Set<TableId> publishedTables) {
        return stubJdbcConnectionWithResults(
                Collections.singletonList(stubPublicationTablesResultSet(publishedTables)));
    }

    private static JdbcConnection stubJdbcConnectionWithResults(List<ResultSet> queryResults) {
        Connection connStub = stubConnection(queryResults);
        return new JdbcConnection(JdbcConfiguration.empty(), config -> connStub, "\"", "\"") {
            @Override
            public synchronized Connection connection() {
                return connStub;
            }
        };
    }

    private static Connection stubConnection(List<ResultSet> queryResults) {
        Deque<ResultSet> remainingResults = new ArrayDeque<>(queryResults);
        return (Connection)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if ("prepareStatement".equals(method.getName())) {
                                return stubPreparedStatement(remainingResults);
                            }
                            if ("close".equals(method.getName())
                                    || "isClosed".equals(method.getName())) {
                                return false;
                            }
                            return defaultReturn(method.getReturnType());
                        });
    }

    private static PreparedStatement stubPreparedStatement(Deque<ResultSet> queryResults) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            if ("executeQuery".equals(method.getName())
                                    && method.getParameterCount() == 0) {
                                return queryResults.removeFirst();
                            }
                            if ("close".equals(method.getName())) {
                                return null;
                            }
                            return defaultReturn(method.getReturnType());
                        });
    }

    private static ResultSet stubPublicationTablesResultSet(Set<TableId> publishedTables) {
        List<Map<String, String>> rows = new java.util.ArrayList<>();
        for (TableId tableId : publishedTables) {
            rows.add(row("schemaname", tableId.schema(), "tablename", tableId.table()));
        }
        return stubRows(rows);
    }

    private static ResultSet stubRows(List<Map<String, String>> rows) {
        Iterator<Map<String, String>> iterator = rows.iterator();
        AtomicReference<Map<String, String>> current = new AtomicReference<>();
        return (ResultSet)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "next":
                                    if (iterator.hasNext()) {
                                        current.set(iterator.next());
                                        return true;
                                    }
                                    current.set(null);
                                    return false;
                                case "getString":
                                    if (args[0] instanceof String) {
                                        return current.get().get((String) args[0]);
                                    }
                                    return null;
                                case "close":
                                    return null;
                                default:
                                    return defaultReturn(method.getReturnType());
                            }
                        });
    }

    private static Map<String, String> row(String... values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            row.put(values[i], values[i + 1]);
        }
        return row;
    }

    private static Object defaultReturn(Class<?> returnType) {
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class || returnType == short.class || returnType == byte.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }
}
