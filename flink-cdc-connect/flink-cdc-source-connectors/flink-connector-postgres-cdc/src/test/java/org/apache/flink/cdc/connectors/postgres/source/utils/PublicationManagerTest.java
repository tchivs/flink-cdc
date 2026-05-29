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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Unit tests for {@link PublicationManager}. */
class PublicationManagerTest {

    private static final TableId CHILD_1 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2 = new TableId(null, "public", "orders_2025");

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

    /**
     * Builds a {@link JdbcConnection} whose {@link JdbcConnection#connection()} returns a {@link
     * Proxy} stub of {@link Connection}, which in turn answers queries against
     * pg_publication_tables with the given set of tables.
     */
    private static JdbcConnection stubJdbcConnection(Set<TableId> publishedTables) {
        Connection connStub = stubConnection(publishedTables);
        return new JdbcConnection(JdbcConfiguration.empty(), config -> connStub, "\"", "\"") {
            @Override
            public synchronized Connection connection() {
                return connStub;
            }
        };
    }

    private static Connection stubConnection(Set<TableId> publishedTables) {
        return (Connection)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if ("prepareStatement".equals(method.getName())) {
                                return stubPreparedStatement(publishedTables);
                            }
                            if ("close".equals(method.getName())
                                    || "isClosed".equals(method.getName())) {
                                return false;
                            }
                            return defaultReturn(method.getReturnType());
                        });
    }

    private static PreparedStatement stubPreparedStatement(Set<TableId> publishedTables) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            if ("executeQuery".equals(method.getName())
                                    && method.getParameterCount() == 0) {
                                return stubResultSet(publishedTables);
                            }
                            if ("close".equals(method.getName())) {
                                return null;
                            }
                            return defaultReturn(method.getReturnType());
                        });
    }

    private static ResultSet stubResultSet(Set<TableId> publishedTables) {
        Iterator<TableId> iterator = publishedTables.iterator();
        TableId[] current = new TableId[1];
        return (ResultSet)
                Proxy.newProxyInstance(
                        PublicationManagerTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "next":
                                    if (iterator.hasNext()) {
                                        current[0] = iterator.next();
                                        return true;
                                    }
                                    current[0] = null;
                                    return false;
                                case "getString":
                                    if (args[0] instanceof String) {
                                        String col = (String) args[0];
                                        if ("schemaname".equals(col)) {
                                            return current[0].schema();
                                        }
                                        if ("tablename".equals(col)) {
                                            return current[0].table();
                                        }
                                    }
                                    return null;
                                case "close":
                                    return null;
                                default:
                                    return defaultReturn(method.getReturnType());
                            }
                        });
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
