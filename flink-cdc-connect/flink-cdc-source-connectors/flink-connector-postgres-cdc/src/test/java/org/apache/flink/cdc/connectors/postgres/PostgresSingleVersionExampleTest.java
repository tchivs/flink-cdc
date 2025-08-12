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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example test class demonstrating single-version PostgreSQL testing.
 *
 * <p>This test class shows how to:
 *
 * <ul>
 *   <li>Use a single PostgreSQL version for testing
 *   <li>Use custom PostgreSQL configuration
 *   <li>Access container through inherited methods
 * </ul>
 */
@PostgresVersion(
        additionalConfig = {"shared_preload_libraries=pg_stat_statements", "log_statement=all"})
public class PostgresSingleVersionExampleTest extends PostgresVersionedTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresSingleVersionExampleTest.class);

    /** Test basic database operations. */
    @TestTemplate
    void testBasicOperations() throws Exception {
        LOG.info("Testing basic operations on PostgreSQL 14");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create a test table
            statement.execute(
                    "CREATE TABLE users ("
                            + "    id SERIAL PRIMARY KEY,"
                            + "    name VARCHAR(100) NOT NULL,"
                            + "    email VARCHAR(100) UNIQUE,"
                            + "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                            + ")");

            // Insert test data
            statement.execute(
                    "INSERT INTO users (name, email) VALUES "
                            + "('Alice', 'alice@example.com'),"
                            + "('Bob', 'bob@example.com'),"
                            + "('Charlie', 'charlie@example.com')");

            // Query the data
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM users");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(3);

            // Test the custom configuration
            ResultSet configRs = statement.executeQuery("SHOW shared_preload_libraries");
            assertThat(configRs.next()).isTrue();
            String preloadLibs = configRs.getString(1);
            assertThat(preloadLibs).contains("pg_stat_statements");
        }

        LOG.info("Basic operations test completed successfully");
    }

    /** Test partition table functionality (PostgreSQL 14 specific features). */
    @TestTemplate
    void testPartitionTables() throws Exception {
        LOG.info("Testing partition table functionality");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create a partitioned table
            statement.execute(
                    "CREATE TABLE orders ("
                            + "    id SERIAL,"
                            + "    order_date DATE NOT NULL,"
                            + "    customer_id INT,"
                            + "    amount DECIMAL(10,2)"
                            + ") PARTITION BY RANGE (order_date)");

            // Create partitions
            statement.execute(
                    "CREATE TABLE orders_2023 PARTITION OF orders "
                            + "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')");

            statement.execute(
                    "CREATE TABLE orders_2024 PARTITION OF orders "
                            + "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");

            // Insert data into partitions
            statement.execute(
                    "INSERT INTO orders (order_date, customer_id, amount) VALUES "
                            + "('2023-06-15', 1, 100.50),"
                            + "('2023-12-25', 2, 250.75),"
                            + "('2024-03-10', 3, 75.25)");

            // Verify data distribution
            ResultSet rs2023 = statement.executeQuery("SELECT COUNT(*) FROM orders_2023");
            assertThat(rs2023.next()).isTrue();
            assertThat(rs2023.getInt(1)).isEqualTo(2);

            ResultSet rs2024 = statement.executeQuery("SELECT COUNT(*) FROM orders_2024");
            assertThat(rs2024.next()).isTrue();
            assertThat(rs2024.getInt(1)).isEqualTo(1);
        }

        LOG.info("Partition table test completed successfully");
    }

    /** Test logical replication setup for CDC. */
    @TestTemplate
    void testLogicalReplicationSetup() throws Exception {
        LOG.info("Testing logical replication setup for CDC");

        String slotName = getSlotName();
        String publicationName = "test_publication_" + System.currentTimeMillis();

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Create a test table
            statement.execute(
                    "CREATE TABLE cdc_test ("
                            + "    id SERIAL PRIMARY KEY,"
                            + "    data TEXT,"
                            + "    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                            + ")");

            // Create a publication for the table
            statement.execute(
                    String.format("CREATE PUBLICATION %s FOR TABLE cdc_test", publicationName));

            // Create a logical replication slot
            statement.execute(
                    String.format(
                            "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                            slotName));

            // Verify the publication
            ResultSet pubRs =
                    statement.executeQuery(
                            String.format(
                                    "SELECT pubname FROM pg_publication WHERE pubname = '%s'",
                                    publicationName));
            assertThat(pubRs.next()).isTrue();
            assertThat(pubRs.getString("pubname")).isEqualTo(publicationName);

            // Verify the replication slot
            ResultSet slotRs =
                    statement.executeQuery(
                            String.format(
                                    "SELECT slot_name, plugin, slot_type FROM pg_replication_slots WHERE slot_name = '%s'",
                                    slotName));
            assertThat(slotRs.next()).isTrue();
            assertThat(slotRs.getString("slot_name")).isEqualTo(slotName);
            assertThat(slotRs.getString("plugin")).isEqualTo("pgoutput");
            assertThat(slotRs.getString("slot_type")).isEqualTo("logical");

            // Insert some test data
            statement.execute(
                    "INSERT INTO cdc_test (data) VALUES "
                            + "('Initial data'),"
                            + "('More data'),"
                            + "('Final data')");

            // Verify data was inserted
            ResultSet dataRs = statement.executeQuery("SELECT COUNT(*) FROM cdc_test");
            assertThat(dataRs.next()).isTrue();
            assertThat(dataRs.getInt(1)).isEqualTo(3);

            // Clean up
            statement.execute(String.format("DROP PUBLICATION %s", publicationName));
            statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", slotName));
        }

        LOG.info("Logical replication setup test completed successfully");
    }

    /** Test WAL level configuration for CDC requirements. */
    @TestTemplate
    void testWalLevelConfiguration() throws Exception {
        LOG.info("Testing WAL level configuration");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Verify WAL level is set to logical (required for CDC)
            ResultSet rs = statement.executeQuery("SHOW wal_level");
            assertThat(rs.next()).isTrue();
            String walLevel = rs.getString(1);
            assertThat(walLevel).isEqualTo("logical");

            // Verify max_replication_slots is configured
            ResultSet slotsRs = statement.executeQuery("SHOW max_replication_slots");
            assertThat(slotsRs.next()).isTrue();
            int maxSlots = slotsRs.getInt(1);
            assertThat(maxSlots).isGreaterThanOrEqualTo(20);

            // Verify pgoutput plugin is available by creating a test slot
            String testSlot = getSlotName();
            statement.execute(
                    String.format(
                            "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                            testSlot));

            // Verify the slot was created successfully
            ResultSet testSlotRs =
                    statement.executeQuery(
                            String.format(
                                    "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s'",
                                    testSlot));
            assertThat(testSlotRs.next()).isTrue();

            // Clean up test slot
            statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", testSlot));
        }

        LOG.info("WAL level configuration test completed successfully");
    }

    /** Test custom PostgreSQL configuration parameters. */
    @TestTemplate
    void testCustomConfiguration() throws Exception {
        LOG.info("Testing custom PostgreSQL configuration");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            // Test shared_preload_libraries configuration
            ResultSet preloadRs = statement.executeQuery("SHOW shared_preload_libraries");
            assertThat(preloadRs.next()).isTrue();
            String preloadLibs = preloadRs.getString(1);
            assertThat(preloadLibs).contains("pg_stat_statements");

            // Test log_statement configuration
            ResultSet logRs = statement.executeQuery("SHOW log_statement");
            assertThat(logRs.next()).isTrue();
            String logStatement = logRs.getString(1);
            assertThat(logStatement).isEqualTo("all");

            // Verify fsync is off (testing optimization)
            ResultSet fsyncRs = statement.executeQuery("SHOW fsync");
            assertThat(fsyncRs.next()).isTrue();
            String fsync = fsyncRs.getString(1);
            assertThat(fsync).isEqualTo("off");
        }

        LOG.info("Custom configuration test completed successfully");
    }
}
