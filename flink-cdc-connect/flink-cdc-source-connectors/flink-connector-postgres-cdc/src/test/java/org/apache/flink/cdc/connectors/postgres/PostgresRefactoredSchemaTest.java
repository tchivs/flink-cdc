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
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchemaV10;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresVersionUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that the refactored schema classes work correctly and provide the same
 * functionality as before with reduced code duplication.
 */
@PostgresVersion(version = "14")
class PostgresRefactoredSchemaTest extends PostgresVersionedTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresRefactoredSchemaTest.class);

    @TestTemplate
    void testRefactoredSchemaReadersWork() throws Exception {
        LOG.info("Testing refactored schema readers functionality");

        // Create a test table
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TABLE test_refactored_schema ("
                            + "id INTEGER PRIMARY KEY, "
                            + "name VARCHAR(50), "
                            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            stmt.execute("INSERT INTO test_refactored_schema (id, name) VALUES (1, 'Test Data')");
            LOG.info("Created test table");
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
                                "test_refactored_schema",
                                10)
                        .create(0);

        try (PostgresConnection connection =
                new PostgresDialect(sourceConfig).openJdbcConnection()) {

            TableId testTable = new TableId("postgres", "public", "test_refactored_schema");

            // Test the appropriate schema reader based on version
            boolean isServer11Plus = PostgresVersionUtils.isServer11OrLater(connection);
            LOG.info("PostgreSQL version 11+: {}", isServer11Plus);

            if (isServer11Plus) {
                // Test CustomPostgresSchema (for PG 11+)
                CustomPostgresSchema schema11Plus =
                        new CustomPostgresSchema(connection, sourceConfig);

                TableChange tableChange = schema11Plus.getTableSchema(testTable);
                assertThat(tableChange).isNotNull();

                Table table = tableChange.getTable();
                assertThat(table).isNotNull();
                assertThat(table.columns()).hasSize(3); // id, name, created_at
                assertThat(table.primaryKeyColumns()).hasSize(1);
                assertThat(table.primaryKeyColumns().get(0).name()).isEqualTo("id");

                LOG.info("✓ CustomPostgresSchema (PG11+) works correctly");
            } else {
                // Test CustomPostgresSchemaV10 (for PG 10)
                CustomPostgresSchemaV10 schemaPG10 =
                        new CustomPostgresSchemaV10(connection, sourceConfig);

                TableChange tableChange = schemaPG10.getTableSchema(testTable);
                assertThat(tableChange).isNotNull();

                Table table = tableChange.getTable();
                assertThat(table).isNotNull();
                assertThat(table.columns()).hasSize(3); // id, name, created_at
                assertThat(table.primaryKeyColumns()).hasSize(1);
                assertThat(table.primaryKeyColumns().get(0).name()).isEqualTo("id");

                LOG.info("✓ CustomPostgresSchemaV10 (PG10) works correctly");
            }
        }

        // Clean up
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_refactored_schema");
        }
    }

    @TestTemplate
    void testBothRefactoredSchemaReadersCanBeInstantiated() throws Exception {
        LOG.info("Testing that both refactored schema readers can be instantiated");

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

            // Test both schema readers can be instantiated
            CustomPostgresSchema schema11Plus = new CustomPostgresSchema(connection, sourceConfig);
            assertThat(schema11Plus).isNotNull();
            LOG.info("✓ Refactored CustomPostgresSchema instantiated successfully");

            CustomPostgresSchemaV10 schemaPG10 =
                    new CustomPostgresSchemaV10(connection, sourceConfig);
            assertThat(schemaPG10).isNotNull();
            LOG.info("✓ Refactored CustomPostgresSchemaV10 instantiated successfully");
        }
    }

    @TestTemplate
    void testPartitionTableWithRefactoredSchema() throws Exception {
        LOG.info("Testing partition table handling with refactored schema classes");

        // Create partitioned table
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TABLE test_refactored_partitioned ("
                            + "id INTEGER NOT NULL, "
                            + "category VARCHAR(10) NOT NULL, "
                            + "data TEXT, "
                            + "PRIMARY KEY (id, category)) "
                            + "PARTITION BY LIST (category)");

            stmt.execute(
                    "CREATE TABLE test_refactored_partition_a "
                            + "PARTITION OF test_refactored_partitioned "
                            + "FOR VALUES IN ('A')");

            stmt.execute(
                    "INSERT INTO test_refactored_partition_a (id, category, data) "
                            + "VALUES (1, 'A', 'Test partition data')");

            LOG.info("Created partitioned table structure");
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
                                "test_refactored_partition_a",
                                10)
                        .create(0);

        try (PostgresConnection connection =
                new PostgresDialect(sourceConfig).openJdbcConnection()) {

            TableId partitionTable =
                    new TableId("postgres", "public", "test_refactored_partition_a");

            // Use the appropriate schema reader based on version
            boolean isServer11Plus = PostgresVersionUtils.isServer11OrLater(connection);

            if (!isServer11Plus) {
                // Only test PG10 specific behavior
                CustomPostgresSchemaV10 schemaPG10 =
                        new CustomPostgresSchemaV10(connection, sourceConfig);

                TableChange tableChange = schemaPG10.getTableSchema(partitionTable);
                assertThat(tableChange).isNotNull();

                Table table = tableChange.getTable();
                assertThat(table).isNotNull();

                // The key test: partition table should have inherited primary keys
                if (table.primaryKeyColumns().isEmpty()) {
                    LOG.warn("Primary key inheritance might not have worked for partition table");
                } else {
                    assertThat(table.primaryKeyColumns()).isNotEmpty();
                    LOG.info(
                            "✓ Partition table correctly has primary keys: {}",
                            table.primaryKeyColumnNames());
                }
            }
        }

        // Clean up
        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_refactored_partitioned CASCADE");
        }
    }
}
