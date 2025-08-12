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

import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresVersionUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the unified PostgreSQL version utilities and partition detection. */
@PostgresVersion(version = "14")
class PostgresVersionUtilsTest extends PostgresVersionedTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresVersionUtilsTest.class);

    @TestTemplate
    void testVersionDetection() throws Exception {
        LOG.info("Testing PostgreSQL version detection utilities");

        PostgresSourceConfig sourceConfig =
                getMockPostgresSourceConfigFactory(
                                new UniqueDatabase(
                                        getContainer(),
                                        "test_db",
                                        "public",
                                        TEST_USER,
                                        TEST_PASSWORD),
                                "public",
                                "test_table",
                                10)
                        .create(0);

        try (PostgresConnection connection =
                new PostgresDialect(sourceConfig).openJdbcConnection()) {

            // Test different version checks based on actual container version
            boolean is10Plus = PostgresVersionUtils.isServer10OrLater(connection);
            boolean is11Plus = PostgresVersionUtils.isServer11OrLater(connection);
            boolean is13Plus = PostgresVersionUtils.isServer13OrLater(connection);
            boolean is94Plus = PostgresVersionUtils.isServer94OrLater(connection);

            LOG.info(
                    "Version checks - 10+: {}, 11+: {}, 13+: {}, 9.4+: {}",
                    is10Plus,
                    is11Plus,
                    is13Plus,
                    is94Plus);

            // All PostgreSQL versions we test should be 9.4+
            assertThat(is94Plus).isTrue();
            // Most should be 10+
            assertThat(is10Plus).isTrue();
        }
    }

    @TestTemplate
    void testPartitionTableDetection() throws Exception {
        LOG.info("Testing partition table detection with unified utilities");

        // Create partitioned table structure
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            // Create parent table with primary key
            stmt.execute(
                    "CREATE TABLE test_partitioned_unified ("
                            + "id INTEGER NOT NULL, "
                            + "category VARCHAR(50) NOT NULL, "
                            + "data TEXT, "
                            + "PRIMARY KEY (id, category)) "
                            + "PARTITION BY LIST (category)");

            // Create partitions
            stmt.execute(
                    "CREATE TABLE test_partition_a "
                            + "PARTITION OF test_partitioned_unified "
                            + "FOR VALUES IN ('A')");

            stmt.execute(
                    "CREATE TABLE test_partition_b "
                            + "PARTITION OF test_partitioned_unified "
                            + "FOR VALUES IN ('B')");

            // Also create a regular table for comparison
            stmt.execute(
                    "CREATE TABLE test_regular_unified ("
                            + "id INTEGER PRIMARY KEY, "
                            + "name VARCHAR(50))");

            LOG.info("Created test table structure");
        }

        PostgresSourceConfig sourceConfig =
                getMockPostgresSourceConfigFactory(
                                new UniqueDatabase(
                                        getContainer(),
                                        "test_db",
                                        "public",
                                        TEST_USER,
                                        TEST_PASSWORD),
                                "public",
                                "test_partitioned_unified",
                                10)
                        .create(0);

        try (PostgresConnection connection =
                new PostgresDialect(sourceConfig).openJdbcConnection()) {

            TableId parentTable = new TableId("postgres", "public", "test_partitioned_unified");
            TableId partitionA = new TableId("postgres", "public", "test_partition_a");
            TableId partitionB = new TableId("postgres", "public", "test_partition_b");
            TableId regularTable = new TableId("postgres", "public", "test_regular_unified");

            // Test partitioned table detection (parent)
            boolean isParentPartitioned =
                    PostgresVersionUtils.isPartitionedTable(connection, parentTable);
            LOG.info("Parent table {} is partitioned: {}", parentTable, isParentPartitioned);
            assertThat(isParentPartitioned).isTrue();

            // Test partition detection (child)
            boolean isPartitionA = PostgresVersionUtils.isPartitionTable(connection, partitionA);
            boolean isPartitionB = PostgresVersionUtils.isPartitionTable(connection, partitionB);
            LOG.info("Table {} is partition: {}", partitionA, isPartitionA);
            LOG.info("Table {} is partition: {}", partitionB, isPartitionB);
            assertThat(isPartitionA).isTrue();
            assertThat(isPartitionB).isTrue();

            // Test regular table (should not be partition/partitioned)
            boolean isRegularPartitioned =
                    PostgresVersionUtils.isPartitionedTable(connection, regularTable);
            boolean isRegularPartition =
                    PostgresVersionUtils.isPartitionTable(connection, regularTable);
            LOG.info(
                    "Regular table {} - partitioned: {}, partition: {}",
                    regularTable,
                    isRegularPartitioned,
                    isRegularPartition);
            assertThat(isRegularPartitioned).isFalse();
            assertThat(isRegularPartition).isFalse();

            // Test parent finding
            Optional<TableId> foundParentA =
                    PostgresVersionUtils.findParentTableForPartition(connection, partitionA);
            Optional<TableId> foundParentB =
                    PostgresVersionUtils.findParentTableForPartition(connection, partitionB);
            assertThat(foundParentA).isPresent();
            assertThat(foundParentB).isPresent();
            assertThat(foundParentA.get()).isEqualTo(parentTable);
            assertThat(foundParentB.get()).isEqualTo(parentTable);
            LOG.info("✓ Successfully found parent tables for partitions");

            // Test primary key detection
            List<String> parentPrimaryKeys =
                    PostgresVersionUtils.getPrimaryKeyColumns(connection, parentTable);
            List<String> regularPrimaryKeys =
                    PostgresVersionUtils.getPrimaryKeyColumns(connection, regularTable);

            assertThat(parentPrimaryKeys).hasSize(2).containsExactly("id", "category");
            assertThat(regularPrimaryKeys).hasSize(1).containsExactly("id");
            LOG.info(
                    "✓ Successfully detected primary keys - parent: {}, regular: {}",
                    parentPrimaryKeys,
                    regularPrimaryKeys);

            // Test child partition finding
            List<TableId> childPartitions =
                    PostgresVersionUtils.findChildPartitions(connection, parentTable);
            assertThat(childPartitions).hasSize(2);
            assertThat(childPartitions).contains(partitionA, partitionB);
            LOG.info("✓ Successfully found child partitions: {}", childPartitions);
        }

        // Clean up
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_partitioned_unified CASCADE");
            stmt.execute("DROP TABLE test_regular_unified");
        }
    }
}
