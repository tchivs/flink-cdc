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

package org.apache.flink.cdc.connectors.postgres.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.postgres.PostgresPg10TestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PG_PORT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCHEMA;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SLOT_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** PG10-specific tests for {@link PostgresDataSourceFactory}. */
class PostgresDataSourceFactoryPg10Test extends PostgresPg10TestBase {

    private static final String SCHEMA_NAME = "inventory_partitioned";
    private static final String PARENT_TABLE_NAME = "products";

    @Test
    void testFactoryConfigDiscoversPg10PartitionedTableWithDatabaseAndSchemaOptions()
            throws Exception {
        String databaseName = "postgres_pg10_pipeline_" + System.nanoTime();
        createDatabase(databaseName);
        initializePg10PartitionedTable(databaseName);

        PostgresDataSource dataSource =
                (PostgresDataSource)
                        new PostgresDataSourceFactory()
                                .createDataSource(
                                        new MockContext(
                                                Configuration.fromMap(options(databaseName))));
        PostgresSourceConfig sourceConfig = dataSource.getPostgresSourceConfig();

        List<TableId> discoveredTables =
                new PostgresDialect(sourceConfig).discoverDataCollections(sourceConfig);

        TableId parentTable = new TableId(null, SCHEMA_NAME, PARENT_TABLE_NAME);
        TableId childUk = new TableId(null, SCHEMA_NAME, "products_uk");
        TableId childUs = new TableId(null, SCHEMA_NAME, "products_us");

        assertThat(sourceConfig.includePartitionedTables()).isTrue();
        assertThat(sourceConfig.getDatabaseList()).containsExactly(databaseName);
        assertThat(sourceConfig.getTableList())
                .containsExactly(SCHEMA_NAME + "." + PARENT_TABLE_NAME);
        assertThat(discoveredTables).containsExactly(parentTable);
        assertThat(sourceConfig.getChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);
        assertThat(sourceConfig.getParentToChildrenMappingOrEmpty())
                .containsEntry(parentTable, List.of(childUk, childUs));
    }

    private Map<String, String> options(String databaseName) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(DATABASE.key(), databaseName);
        options.put(SCHEMA.key(), SCHEMA_NAME);
        options.put(TABLES.key(), PARENT_TABLE_NAME);
        options.put(SLOT_NAME.key(), getSlotName());
        options.put(SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED.key(), "true");
        options.put(PostgresDataSourceOptions.DECODING_PLUGIN_NAME.key(), "pgoutput");
        return options;
    }

    private void createDatabase(String databaseName) throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
    }

    private void initializePg10PartitionedTable(String databaseName) throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + SCHEMA_NAME);
            statement.execute(
                    "CREATE TABLE "
                            + SCHEMA_NAME
                            + ".products ("
                            + " id SERIAL NOT NULL,"
                            + " name VARCHAR(255) NOT NULL DEFAULT 'flink',"
                            + " description VARCHAR(512),"
                            + " weight FLOAT,"
                            + " country VARCHAR(20) NOT NULL"
                            + ") PARTITION BY LIST(country)");
            statement.execute("ALTER TABLE " + SCHEMA_NAME + ".products REPLICA IDENTITY FULL");
            statement.execute(
                    "CREATE TABLE "
                            + SCHEMA_NAME
                            + ".products_uk PARTITION OF "
                            + SCHEMA_NAME
                            + ".products FOR VALUES IN ('uk')");
            statement.execute(
                    "CREATE TABLE "
                            + SCHEMA_NAME
                            + ".products_us PARTITION OF "
                            + SCHEMA_NAME
                            + ".products FOR VALUES IN ('us')");
        }
    }

    private static class MockContext implements Factory.Context {

        private final Configuration factoryConfiguration;

        private MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }
    }
}
