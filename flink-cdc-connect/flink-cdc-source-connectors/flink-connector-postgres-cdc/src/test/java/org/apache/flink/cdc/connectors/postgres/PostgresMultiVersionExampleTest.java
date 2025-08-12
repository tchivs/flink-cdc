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

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;

import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example test class demonstrating the new multi-version PostgreSQL testing framework.
 *
 * <p>This test class shows how to:
 *
 * <ul>
 *   <li>Use multiple PostgreSQL versions in a single test class
 *   <li>Access container instances through method parameters
 *   <li>Use the inherited utility methods from PostgresVersionedTestBase
 *   <li>Test version-specific features
 * </ul>
 */
@PostgresVersion(version = "10")
@PostgresVersion(version = "11")
@PostgresVersion(version = "12")
@PostgresVersion(version = "13")
@PostgresVersion(version = "14")
@PostgresVersion(version = "15")
@PostgresVersion(version = "16")
public class PostgresMultiVersionExampleTest extends PostgresVersionedTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresMultiVersionExampleTest.class);

    /**
     * Test basic connectivity across all PostgreSQL versions. This test will run on PostgreSQL 12,
     * 13, 14, 15, and 16.
     */
    @TestTemplate
    void testBasicConnectivity(PostgreSQLContainer<?> container, Network network) throws Exception {
        LOG.info("Testing basic connectivity on PostgreSQL version: {}", getPostgresVersion());

        // Test direct container parameter injection
        assertThat(container).isNotNull();
        assertThat(container.isRunning()).isTrue();
        assertThat(network).isNotNull();

        // Test inherited utility methods
        try (Connection connection = getJdbcConnection()) {
            assertThat(connection).isNotNull();
            assertThat(connection.isValid(5)).isTrue();

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT 1 as test_value");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("test_value")).isEqualTo(1);
            }
        }

        LOG.info("Basic connectivity test passed for PostgreSQL version: {}", getPostgresVersion());
    }

    /**
     * Test version-specific features. This demonstrates how to conditionally test features based on
     * PostgreSQL version.
     */
    @TestTemplate
    void testVersionSpecificFeatures() throws Exception {
        String version = getPostgresVersion();
        LOG.info("Testing version-specific features on: {}", version);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Test logical replication support (available in all tested versions)
            ResultSet rs = statement.executeQuery("SHOW wal_level");
            assertThat(rs.next()).isTrue();
            String walLevel = rs.getString(1);
            assertThat(walLevel).isEqualTo("logical");

            // Test version-specific features
            if (isPostgresVersionAtLeast(13)) {
                LOG.info("Testing PostgreSQL 13+ features");
                // Test features available in PostgreSQL 13+
                // For example, extended statistics
                statement.execute("CREATE TABLE test_table (id int, name text)");
                statement.execute("CREATE STATISTICS test_stats ON id, name FROM test_table");

                // Verify the statistics was created
                ResultSet statsRs =
                        statement.executeQuery(
                                "SELECT stxname FROM pg_statistic_ext WHERE stxname = 'test_stats'");
                assertThat(statsRs.next()).isTrue();
                assertThat(statsRs.getString("stxname")).isEqualTo("test_stats");
            }

            if (isPostgresVersionAtLeast(14)) {
                LOG.info("Testing PostgreSQL 14+ features");
                // Test features available in PostgreSQL 14+
                // For example, multirange types
                ResultSet typeRs =
                        statement.executeQuery(
                                "SELECT typname FROM pg_type WHERE typname = 'int4multirange'");
                assertThat(typeRs.next()).isTrue();
            }

            if (isPostgresVersionAtLeast(15)) {
                LOG.info("Testing PostgreSQL 15+ features");
                // Test features available in PostgreSQL 15+
                // For example, MERGE command support
                statement.execute("CREATE TABLE target_table (id int PRIMARY KEY, value text)");
                statement.execute("CREATE TABLE source_table (id int, value text)");
                statement.execute("INSERT INTO source_table VALUES (1, 'test')");

                // MERGE command (PostgreSQL 15+)
                statement.execute(
                        "MERGE INTO target_table t "
                                + "USING source_table s ON t.id = s.id "
                                + "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)");

                ResultSet mergeRs = statement.executeQuery("SELECT COUNT(*) FROM target_table");
                assertThat(mergeRs.next()).isTrue();
                assertThat(mergeRs.getInt(1)).isEqualTo(1);
            }
        }

        LOG.info("Version-specific features test passed for: {}", version);
    }

    /**
     * Test replication slot creation across versions. This is particularly important for CDC
     * functionality.
     */
    @TestTemplate
    void testReplicationSlotCreation() throws Exception {
        LOG.info(
                "Testing replication slot creation on PostgreSQL version: {}",
                getPostgresVersion());

        String slotName = getSlotName();

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create a logical replication slot
            statement.execute(
                    String.format(
                            "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                            slotName));

            // Verify the slot was created
            ResultSet rs =
                    statement.executeQuery(
                            String.format(
                                    "SELECT slot_name, plugin FROM pg_replication_slots WHERE slot_name = '%s'",
                                    slotName));
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("slot_name")).isEqualTo(slotName);
            assertThat(rs.getString("plugin")).isEqualTo("pgoutput");

            // Clean up the slot
            statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", slotName));

            LOG.info(
                    "Replication slot test passed for PostgreSQL version: {}",
                    getPostgresVersion());
        }
    }

    /** Nested test class example showing how the framework works with @Nested tests. */
    @PostgresVersion(version = "14")
    @PostgresVersion(version = "16")
    public static class NestedTests extends PostgresVersionedTestBase {

        @TestTemplate
        void testNestedFunctionality() throws Exception {
            LOG.info(
                    "Testing nested functionality on PostgreSQL version: {}", getPostgresVersion());

            // This test will only run on PostgreSQL 14 and 16
            assertThat(isPostgresVersionAtLeast(14)).isTrue();

            try (Connection connection = getJdbcConnection()) {
                assertThat(connection.isValid(5)).isTrue();
            }

            LOG.info("Nested test passed for PostgreSQL version: {}", getPostgresVersion());
        }
    }
}
