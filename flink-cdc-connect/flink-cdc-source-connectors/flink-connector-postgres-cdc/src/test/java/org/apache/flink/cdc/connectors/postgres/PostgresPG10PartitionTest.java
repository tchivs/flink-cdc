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
 * Test PostgreSQL 10 partition table handling and primary key constraints. This test verifies that
 * partition tables in PostgreSQL 10 work correctly with primary keys defined on child tables, since
 * PostgreSQL 10 does not support primary keys directly on parent partitioned tables.
 */
@PostgresVersion(version = "10")
class PostgresPG10PartitionTest extends PostgresVersionedTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPG10PartitionTest.class);

    @TestTemplate
    void testPartitionTablePrimaryKeyInheritance() throws Exception {
        LOG.info("Testing PostgreSQL 10 partition table primary key handling");

        // Create partitioned table following PostgreSQL 10 constraints
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            // Create parent partitioned table WITHOUT primary key (PG10 limitation)
            stmt.execute(
                    "CREATE TABLE test_partitioned ("
                            + "id INTEGER NOT NULL, "
                            + "name VARCHAR(50), "
                            + "created_date DATE NOT NULL) "
                            + "PARTITION BY RANGE (created_date)");

            // Create partition (child table)
            stmt.execute(
                    "CREATE TABLE test_partitioned_2023 "
                            + "PARTITION OF test_partitioned "
                            + "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')");

            // Add primary key to the partition table (this is supported in PG10)
            stmt.execute(
                    "ALTER TABLE test_partitioned_2023 " + "ADD PRIMARY KEY (id, created_date)");

            // Insert test data
            stmt.execute(
                    "INSERT INTO test_partitioned_2023 (id, name, created_date) "
                            + "VALUES (1, 'Test User', '2023-06-15')");

            LOG.info("Created partition table structure with primary key on child table");
        }

        // Verify that the partition table was created successfully with primary key
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            // Verify the partition table exists and has data
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_partitioned_2023");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);

            // Verify primary key constraint exists on the partition table
            ResultSet pkRs =
                    stmt.executeQuery(
                            "SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'test_partitioned_2023' AND constraint_type = 'PRIMARY KEY'");
            assertThat(pkRs.next()).isTrue();
            assertThat(pkRs.getString("constraint_name")).isNotNull();

            // Verify the primary key columns
            ResultSet colRs =
                    stmt.executeQuery(
                            "SELECT column_name FROM information_schema.key_column_usage "
                                    + "WHERE table_name = 'test_partitioned_2023' AND constraint_name IN "
                                    + "(SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'test_partitioned_2023' AND constraint_type = 'PRIMARY KEY') "
                                    + "ORDER BY ordinal_position");

            assertThat(colRs.next()).isTrue();
            assertThat(colRs.getString("column_name")).isEqualTo("id");
            assertThat(colRs.next()).isTrue();
            assertThat(colRs.getString("column_name")).isEqualTo("created_date");

            LOG.info("✓ Successfully verified partition table with primary key constraint");
        }

        // Clean up
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_partitioned CASCADE");
        }
    }

    @TestTemplate
    void testNonPartitionTableNotAffected() throws Exception {
        LOG.info("Testing that non-partition tables are not affected by PG10 enhancements");

        // Create regular table with primary key
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TABLE test_regular ("
                            + "id INTEGER PRIMARY KEY, "
                            + "name VARCHAR(50))");

            stmt.execute("INSERT INTO test_regular (id, name) VALUES (1, 'Regular Table')");
        }

        // Verify that regular tables work normally
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            // Verify the regular table exists and has data
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_regular");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);

            // Verify primary key constraint exists on the regular table
            ResultSet pkRs =
                    stmt.executeQuery(
                            "SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'test_regular' AND constraint_type = 'PRIMARY KEY'");
            assertThat(pkRs.next()).isTrue();
            assertThat(pkRs.getString("constraint_name")).isNotNull();

            LOG.info("✓ Regular table works correctly with primary key");
        }

        // Clean up
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_regular");
        }
    }
}
