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
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

        TableId parent = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        Assertions.assertThat(dialectOfInventoryPartitionedDatabase.getChildToParentMapping())
                .containsEntry(childUk, parent)
                .containsEntry(childUs, parent);

        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialectOfInventoryPartitionedDatabase.discoverDataCollectionSchemas(
                        configFactoryOfInventoryPartitionedDatabase.create(0));
        Assertions.assertThat(tableSchemas)
                .containsOnlyKeys(parent)
                .doesNotContainKeys(childUk, childUs);

        PartitionCaptureState childResolvedState =
                dialectOfInventoryPartitionedDatabase.discoverPartitionState(
                        tableIdsOfInventoryPartitionedDatabase);
        Assertions.assertThat(childResolvedState.getChildToParent())
                .containsEntry(childUk, parent)
                .containsEntry(childUs, parent);
    }

    @Test
    void testPartitionChildSplitMatchesParentTableFilter() {
        inventoryPartitionedDatabase.createAndInitialize();

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        PostgresDialect dialect = new PostgresDialect(configFactory.create(0));
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");

        Assertions.assertThat(dialect.isIncludeDataCollection(configFactory.create(0), childUk))
                .isTrue();
        Assertions.assertThat(dialect.isIncludeDataCollection(configFactory.create(0), childUs))
                .isTrue();

        PostgresSourceConfigFactory disabledConfigFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        PostgresDialect disabledDialect = new PostgresDialect(disabledConfigFactory.create(0));
        Assertions.assertThat(
                        disabledDialect.isIncludeDataCollection(
                                disabledConfigFactory.create(0), childUk))
                .isFalse();
    }
}
