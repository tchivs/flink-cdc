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
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresSchemaReader;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that the correct PostgreSQL schema reader is used based on the PostgreSQL server
 * version.
 */
@PostgresVersion(version = "14")
class PostgresSchemaVersionTest extends PostgresVersionedTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaVersionTest.class);

    @TestTemplate
    void testSchemaReaderSelection() throws Exception {
        LOG.info("Testing PostgreSQL schema reader version selection");

        // Get the PostgreSQL version from the container
        String version = getPostgreSQLVersion(getContainer());
        LOG.info("PostgreSQL version: {}", version);

        // Create a mock PostgresSourceConfig (simplified for test)
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

        // Create PostgresDialect and test the schema reader selection
        PostgresDialect dialect = new PostgresDialect(sourceConfig);

        // Open connection to initialize server version detection
        PostgresConnection connection = dialect.openJdbcConnection();

        // Use reflection to access the private createSchemaReader method for testing
        Method createSchemaReaderMethod =
                PostgresDialect.class.getDeclaredMethod(
                        "createSchemaReader", PostgresConnection.class);
        createSchemaReaderMethod.setAccessible(true);

        PostgresSchemaReader schemaReader =
                (PostgresSchemaReader) createSchemaReaderMethod.invoke(dialect, connection);

        // Verify the correct schema reader type is selected
        if (version.startsWith("10.")) {
            assertThat(schemaReader).isInstanceOf(CustomPostgresSchemaV10.class);
            LOG.info("✓ Correctly selected CustomPostgresSchemaV10 for PostgreSQL 10");
        } else {
            assertThat(schemaReader).isInstanceOf(CustomPostgresSchema.class);
            LOG.info("✓ Correctly selected CustomPostgresSchema for PostgreSQL 11+");
        }

        // Verify schema reader is functional
        assertThat(schemaReader).isNotNull();

        connection.close();
    }

    @TestTemplate
    void testBothSchemaReadersCanBeInstantiated(PostgreSQLContainer<?> container) throws Exception {
        LOG.info("Testing that both schema reader types can be instantiated");

        PostgresSourceConfig sourceConfig =
                getMockPostgresSourceConfigFactory(
                                new UniqueDatabase(
                                        container, "test_db", "public", TEST_USER, TEST_PASSWORD),
                                "public",
                                "test_table",
                                10)
                        .create(0);

        try (PostgresConnection connection =
                new PostgresDialect(sourceConfig).openJdbcConnection()) {

            // Test CustomPostgresSchema (for PG 11+)
            CustomPostgresSchema schema11Plus = new CustomPostgresSchema(connection, sourceConfig);
            assertThat(schema11Plus).isNotNull();
            LOG.info("✓ CustomPostgresSchema instantiated successfully");

            // Test CustomPostgresSchemaV10 (for PG 10)
            CustomPostgresSchemaV10 schemaPG10 =
                    new CustomPostgresSchemaV10(connection, sourceConfig);
            assertThat(schemaPG10).isNotNull();
            LOG.info("✓ CustomPostgresSchemaV10 instantiated successfully");
        }
    }

    private String getPostgreSQLVersion(PostgreSQLContainer<?> container) throws Exception {
        try (Connection conn = getJdbcConnection(container);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT version()")) {

            if (rs.next()) {
                String fullVersion = rs.getString(1);
                // Extract version number (e.g., "PostgreSQL 14.2" -> "14.2")
                if (fullVersion.contains("PostgreSQL")) {
                    String[] parts = fullVersion.split("\\s+");
                    if (parts.length >= 2) {
                        return parts[1];
                    }
                }
                return fullVersion;
            }
            return "unknown";
        }
    }
}
