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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionPublicationRefresher;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Tests for {@link PostgresDialect}. */
class PostgresDialectTest extends PostgresTestBase {

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres1",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres2",
                    "inventory",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private final UniqueDatabase inventoryPartitionedDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres3",
                    "inventory_partitioned",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private final UniqueDatabase inventoryPartitionedPublicationDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres4",
                    "inventory_partitioned",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    void testDiscoverDataCollectionsInMultiDatabases() {

        // initial two databases in same postgres instance
        customDatabase.createAndInitialize();
        inventoryDatabase.createAndInitialize();

        // get table named 'customer.customers' from customDatabase which is actual in
        // inventoryDatabase
        PostgresSourceConfigFactory configFactoryOfCustomDatabase =
                getMockPostgresSourceConfigFactory(customDatabase, "customer", "Customers", 10);
        PostgresDialect dialectOfcustomDatabase =
                new PostgresDialect(configFactoryOfCustomDatabase.create(0));
        List<TableId> tableIdsOfcustomDatabase =
                dialectOfcustomDatabase.discoverDataCollections(
                        configFactoryOfCustomDatabase.create(0));
        Assertions.assertThat(tableIdsOfcustomDatabase.get(0)).hasToString("customer.Customers");

        // get table named 'inventory.products' from customDatabase which is actual in
        // inventoryDatabase
        // however, nothing is found
        PostgresSourceConfigFactory configFactoryOfInventoryDatabase =
                getMockPostgresSourceConfigFactory(inventoryDatabase, "inventory", "products", 10);
        PostgresDialect dialectOfInventoryDatabase =
                new PostgresDialect(configFactoryOfInventoryDatabase.create(0));
        List<TableId> tableIdsOfInventoryDatabase =
                dialectOfInventoryDatabase.discoverDataCollections(
                        configFactoryOfInventoryDatabase.create(0));
        Assertions.assertThat(tableIdsOfInventoryDatabase.get(0)).hasToString("inventory.products");

        // get table named 'customer.customers' from customDatabase which is actual not in
        // customDatabase
        // however, something is found
        PostgresSourceConfigFactory configFactoryOfInventoryDatabase2 =
                getMockPostgresSourceConfigFactory(inventoryDatabase, "customer", "customers", 10);
        PostgresDialect dialectOfInventoryDatabase2 =
                new PostgresDialect(configFactoryOfInventoryDatabase2.create(0));
        List<TableId> tableIdsOfInventoryDatabase2 =
                dialectOfInventoryDatabase2.discoverDataCollections(
                        configFactoryOfInventoryDatabase2.create(0));
        Assertions.assertThat(tableIdsOfInventoryDatabase2).isEmpty();
    }

    @Test
    void testDiscoverDataCollectionsForPartitionedTable() {
        // initial database with partitioned table
        inventoryPartitionedDatabase.createAndInitialize();

        // get table named 'inventory_partitioned.products' from inventoryPartitionedDatabase
        PostgresSourceConfigFactory configFactoryOfInventoryPartitionedDatabase =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactoryOfInventoryPartitionedDatabase.setIncludePartitionedTables(true);
        PostgresDialect dialectOfInventoryPartitionedDatabase =
                new PostgresDialect(configFactoryOfInventoryPartitionedDatabase.create(0));
        List<TableId> tableIdsOfInventoryPartitionedDatabase =
                dialectOfInventoryPartitionedDatabase.discoverDataCollections(
                        configFactoryOfInventoryPartitionedDatabase.create(0));
        Assertions.assertThat(tableIdsOfInventoryPartitionedDatabase)
                .extracting(TableId::toString)
                .containsExactlyInAnyOrder(
                        "inventory_partitioned.products_uk", "inventory_partitioned.products_us");
        Assertions.assertThat(
                        dialectOfInventoryPartitionedDatabase
                                .routingState()
                                .routeToLogicalTable(
                                        new TableId(null, "inventory_partitioned", "products_uk")))
                .isEqualTo(new TableId(null, "inventory_partitioned", "products"));
    }

    @Test
    void testDiscoverDataCollectionsRejectsDecoderbufsWithPublishViaPartitionRoot()
            throws Exception {
        inventoryPartitionedPublicationDatabase.createAndInitialize();
        createPublication("cdc_pub_pvpr", true);
        createPublication("cdc_pub_child_identity", false);

        PostgresSourceConfigFactory decoderbufsConfigFactory =
                createPartitionedConfigFactory("decoderbufs", "cdc_pub_pvpr");
        PostgresDialect decoderbufsDialect =
                new PostgresDialect(decoderbufsConfigFactory.create(0));
        Assertions.assertThatThrownBy(
                        () ->
                                decoderbufsDialect.discoverDataCollections(
                                        decoderbufsConfigFactory.create(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_005)
                .hasMessageContaining("decoding.plugin.name=decoderbufs")
                .hasMessageContaining("publish_via_partition_root=true");

        PostgresSourceConfigFactory pgoutputConfigFactory =
                createPartitionedConfigFactory("pgoutput", "cdc_pub_pvpr");
        PostgresDialect pgoutputDialect = new PostgresDialect(pgoutputConfigFactory.create(0));
        Assertions.assertThat(
                        pgoutputDialect.discoverDataCollections(pgoutputConfigFactory.create(0)))
                .extracting(TableId::toString)
                .containsExactlyInAnyOrder(
                        "inventory_partitioned.products_uk", "inventory_partitioned.products_us");

        PostgresSourceConfigFactory decoderbufsWithoutPvprConfigFactory =
                createPartitionedConfigFactory("decoderbufs", "cdc_pub_child_identity");
        PostgresDialect decoderbufsWithoutPvprDialect =
                new PostgresDialect(decoderbufsWithoutPvprConfigFactory.create(0));
        Assertions.assertThat(
                        decoderbufsWithoutPvprDialect.discoverDataCollections(
                                decoderbufsWithoutPvprConfigFactory.create(0)))
                .extracting(TableId::toString)
                .containsExactlyInAnyOrder(
                        "inventory_partitioned.products_uk", "inventory_partitioned.products_us");
    }

    @Test
    void testDiscoverDataCollectionsRejectsDecoderbufsWithFilteredPublicationMode() {
        inventoryPartitionedPublicationDatabase.createAndInitialize();

        PostgresSourceConfigFactory decoderbufsConfigFactory =
                createPartitionedConfigFactory("decoderbufs", "cdc_pub_filtered");
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(
                PostgresConnectorConfig.PUBLICATION_NAME.name(), "cdc_pub_filtered");
        dbzProperties.setProperty(
                PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(), "filtered");
        decoderbufsConfigFactory.debeziumProperties(dbzProperties);
        PostgresDialect decoderbufsDialect =
                new PostgresDialect(decoderbufsConfigFactory.create(0));

        Assertions.assertThatThrownBy(
                        () ->
                                decoderbufsDialect.discoverDataCollections(
                                        decoderbufsConfigFactory.create(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_006)
                .hasMessageContaining("decoding.plugin.name=decoderbufs")
                .hasMessageContaining("debezium.publication.autocreate.mode=filtered")
                .hasMessageContaining("inventory_partitioned.products")
                .hasMessageContaining("Remediation");
    }

    @Test
    void testDiscoverDataCollectionsRefreshesDisabledPublicationMembership() throws Exception {
        inventoryPartitionedPublicationDatabase.createAndInitialize();
        createEmptyPublication("cdc_pub_refresh");

        PostgresSourceConfigFactory pgoutputConfigFactory =
                createPartitionedConfigFactory("pgoutput", "cdc_pub_refresh");
        pgoutputConfigFactory.setPartitionPublicationRefreshEnabled(true);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(
                PostgresConnectorConfig.PUBLICATION_NAME.name(), "cdc_pub_refresh");
        dbzProperties.setProperty(
                PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(), "disabled");
        pgoutputConfigFactory.debeziumProperties(dbzProperties);

        PostgresDialect pgoutputDialect = new PostgresDialect(pgoutputConfigFactory.create(0));
        Assertions.assertThat(
                        pgoutputDialect.discoverDataCollections(pgoutputConfigFactory.create(0)))
                .extracting(TableId::toString)
                .containsExactlyInAnyOrder(
                        "inventory_partitioned.products_uk", "inventory_partitioned.products_us");
        Assertions.assertThat(publicationTables("cdc_pub_refresh"))
                .containsExactlyInAnyOrder(
                        "inventory_partitioned.products_uk", "inventory_partitioned.products_us");
    }

    private PostgresSourceConfigFactory createPartitionedConfigFactory(
            String pluginName, String publicationName) {
        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedPublicationDatabase,
                        "inventory_partitioned",
                        "products",
                        10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.decodingPluginName(pluginName);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(PostgresConnectorConfig.PUBLICATION_NAME.name(), publicationName);
        configFactory.debeziumProperties(dbzProperties);
        return configFactory;
    }

    private void createEmptyPublication(String publicationName) throws Exception {
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedPublicationDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE PUBLICATION %s", publicationName));
        }
    }

    private void createPublication(String publicationName, boolean publishViaPartitionRoot)
            throws Exception {
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedPublicationDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products "
                                    + "WITH (publish_via_partition_root=%s)",
                            publicationName, publishViaPartitionRoot));
        }
    }

    private List<String> publicationTables(String publicationName) throws Exception {
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedPublicationDatabase.getDatabaseName());
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT schemaname, tablename FROM pg_publication_tables "
                                                + "WHERE pubname = '%s'",
                                        publicationName))) {
            List<String> publicationTables = new ArrayList<>();
            while (rs.next()) {
                publicationTables.add(rs.getString("schemaname") + "." + rs.getString("tablename"));
            }
            return publicationTables;
        }
    }
}
