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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.PostgresPg10TestBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10CaptureState;
import org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamingSessionRuntime;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionMapper;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionReconciler;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PublicationManager;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/** PG10-specific tests for {@link PostgresDialect}. */
class PostgresDialectPg10Test extends PostgresPg10TestBase {

    @Test
    void testRefreshResultContainsOnlyNewChildren() {
        TableId parentTable = new TableId(null, "schema", "products");
        TableId childUk = new TableId(null, "schema", "products_uk");
        TableId childUs = new TableId(null, "schema", "products_us");
        TableId childCa = new TableId(null, "schema", "products_ca");

        Map<TableId, List<TableId>> oldParentToChildren = new HashMap<>();
        oldParentToChildren.put(parentTable, List.of(childUk, childUs));

        Map<TableId, TableId> oldChildToParent = new HashMap<>();
        oldChildToParent.put(childUk, parentTable);
        oldChildToParent.put(childUs, parentTable);

        Map<TableId, List<TableId>> newParentToChildren = new HashMap<>();
        newParentToChildren.put(parentTable, List.of(childUk, childUs, childCa));

        Map<TableId, TableId> newChildToParent = new HashMap<>();
        newChildToParent.put(childUk, parentTable);
        newChildToParent.put(childUs, parentTable);
        newChildToParent.put(childCa, parentTable);

        Pg10PartitionReconciler.Pg10ReconcileResult refreshResult =
                Pg10PartitionReconciler.Pg10ReconcileResult.from(
                        oldParentToChildren,
                        oldChildToParent,
                        newParentToChildren,
                        newChildToParent);

        Assertions.assertThat(refreshResult.getNewChildren()).containsExactly(childCa);
        Assertions.assertThat(refreshResult.getNewChildToParent())
                .containsEntry(childCa, parentTable);
    }

    @Test
    void testReconcilesPg10PartitionMappingsWhenNewChildAppears() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        List<TableId> parentTables = dialect.discoverDataCollections(config);

        Assertions.assertThat(parentTables).containsExactly(parentTable);
        Assertions.assertThat(config.getChildToParentMapping())
                .containsEntry(childUk, parentTable)
                .containsEntry(childUs, parentTable);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        Pg10PartitionReconciler.Pg10ReconcileResult refreshResult;
        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            refreshResult =
                    dialect.reconcilePg10PartitionMappings(
                            jdbc,
                            Pg10CaptureState.of(
                                    config.getChildToParentMappingOrEmpty(),
                                    config.getParentToChildrenMappingOrEmpty()),
                            parentTables);
        }

        Assertions.assertThat(refreshResult.getNewChildren()).containsExactly(childCa);
        Assertions.assertThat(refreshResult.getNewChildToParent())
                .containsEntry(childCa, parentTable);

        Assertions.assertThat(refreshResult.getLatestChildToParent())
                .containsEntry(childCa, parentTable);
        Assertions.assertThat(refreshResult.getLatestParentToChildren().get(parentTable))
                .contains(childCa);
    }

    @Test
    void testSequentialReconcileUsesLatestStateAsPublicationPollerBaseline() {
        TableId parentTable = new TableId(null, "schema", "products");
        TableId childUk = new TableId(null, "schema", "products_uk");
        TableId childCa = new TableId(null, "schema", "products_ca");
        TableId childAu = new TableId(null, "schema", "products_au");

        Map<TableId, List<TableId>> initialParentToChildren = new HashMap<>();
        initialParentToChildren.put(parentTable, List.of(childUk));
        Map<TableId, TableId> initialChildToParent = new HashMap<>();
        initialChildToParent.put(childUk, parentTable);

        Map<TableId, List<TableId>> afterCaParentToChildren = new HashMap<>();
        afterCaParentToChildren.put(parentTable, List.of(childUk, childCa));
        Pg10PartitionReconciler.Pg10ReconcileResult afterCa =
                Pg10PartitionReconciler.Pg10ReconcileResult.from(
                        initialParentToChildren,
                        initialChildToParent,
                        afterCaParentToChildren,
                        Pg10PartitionMapper.buildChildToParentMapping(afterCaParentToChildren));

        Map<TableId, List<TableId>> afterAuParentToChildren = new HashMap<>();
        afterAuParentToChildren.put(parentTable, List.of(childUk, childCa, childAu));
        Pg10PartitionReconciler.Pg10ReconcileResult afterAu =
                Pg10PartitionReconciler.Pg10ReconcileResult.from(
                        afterCa.getLatestParentToChildren(),
                        afterCa.getLatestChildToParent(),
                        afterAuParentToChildren,
                        Pg10PartitionMapper.buildChildToParentMapping(afterAuParentToChildren));

        Pg10PartitionReconciler.Pg10ReconcileResult afterAuWithStaleBaseline =
                Pg10PartitionReconciler.Pg10ReconcileResult.from(
                        initialParentToChildren,
                        initialChildToParent,
                        afterAuParentToChildren,
                        Pg10PartitionMapper.buildChildToParentMapping(afterAuParentToChildren));

        Assertions.assertThat(afterCa.getNewChildren()).containsExactly(childCa);
        Assertions.assertThat(afterAuWithStaleBaseline.getNewChildren())
                .containsExactlyInAnyOrder(childCa, childAu);
        Assertions.assertThat(afterAu.getNewChildren()).containsExactly(childAu);
    }

    @Test
    void testPublicationValidationDiagnosticsIncludeAllMissingChildren() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_pg10_validation_diagnostics";

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products_uk",
                            publicationName));
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("publication.name", publicationName);
        configFactory.debeziumProperties(debeziumProperties);

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        Assertions.assertThatThrownBy(() -> dialect.discoverDataCollections(config))
                .hasMessageContaining("PG10 partition publication validation failed")
                .hasMessageContaining("inventory_partitioned.products_us")
                .hasMessageContaining("inventory_partitioned.products_ca")
                .hasMessageContaining(publicationName);
    }

    @Test
    void testComputesMissingPublicationChildrenForRuntimePartitions() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_pg10_runtime_reconcile";
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        List<TableId> runtimeChildren = List.of(childUk, childUs);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products_uk",
                            publicationName));
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            List<TableId> missingChildren =
                    Pg10PartitionMapper.findChildrenMissingFromPublication(
                            jdbc, publicationName, runtimeChildren);

            Assertions.assertThat(missingChildren).containsExactly(childUs);

            Pg10PartitionMapper.addChildrenToPublication(jdbc, publicationName, missingChildren);

            Assertions.assertThat(
                            Pg10PartitionMapper.findChildrenMissingFromPublication(
                                    jdbc, publicationName, runtimeChildren))
                    .isEmpty();
        }
    }

    @Test
    void testAddingRuntimeChildToPublicationIsVisibleAcrossConnections() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_pg10_cross_connection_visibility";
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us",
                            publicationName));
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Pg10PublicationManager.addTablesToPublication(
                    jdbc, publicationName, Collections.singletonList(childCa));
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Assertions.assertThat(
                            Pg10PublicationManager.findMissingPublicationChildren(
                                    jdbc, publicationName, Collections.singletonList(childCa)))
                    .isEmpty();
        }
    }

    @Test
    void testReconcilerReportsNewChildrenWithoutSideEffects() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_reconciler_test";
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    "CREATE PUBLICATION "
                            + publicationName
                            + " FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us");
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");

        List<TableId> parentTables = dialect.discoverDataCollections(config);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult;
        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            reconcileResult = dialect.reconcilePg10PartitionMappings(jdbc, config, parentTables);
        }

        Assertions.assertThat(reconcileResult.getNewChildren())
                .containsExactly(new TableId(null, "inventory_partitioned", "products_ca"));

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            List<TableId> missingInPublication =
                    Pg10PublicationManager.findMissingPublicationChildren(
                            jdbc,
                            publicationName,
                            Collections.singletonList(
                                    new TableId(null, "inventory_partitioned", "products_ca")));
            Assertions.assertThat(missingInPublication)
                    .containsExactly(new TableId(null, "inventory_partitioned", "products_ca"));
        }
    }

    @Test
    void testAcceptedCaptureStateSeedsNextFetchContextInitialState() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        List<TableId> parentTables = dialect.discoverDataCollections(config);
        StreamSplit streamSplit = createMinimalStreamSplit(config, dialect);
        PostgresSourceFetchTaskContext fetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);

        Pg10CaptureState initialCaptureState =
                fetchTaskContext.buildInitialCaptureState(streamSplit);

        Assertions.assertThat(initialCaptureState.getChildToParentMapping())
                .doesNotContainKey(childCa);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        Pg10CaptureState acceptedCaptureState;
        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                    dialect.reconcilePg10PartitionMappings(jdbc, initialCaptureState, parentTables);
            acceptedCaptureState =
                    Pg10CaptureState.of(
                            reconcileResult.getLatestChildToParent(),
                            reconcileResult.getLatestParentToChildren());
        }

        fetchTaskContext.acceptPg10CaptureStateForRestart(
                acceptedCaptureState, loadOffsetContext(config, 67890L, 67890L), streamSplit);

        Assertions.assertThat(config.isPg10PartitionMappingInitialized()).isTrue();
        Assertions.assertThat(config.getChildToParentMappingOrEmpty())
                .containsEntry(childCa, parentTable);
        Assertions.assertThat(config.getParentToChildrenMappingOrEmpty().get(parentTable))
                .contains(childCa);
        Assertions.assertThat(
                        fetchTaskContext
                                .getDbzConnectorConfig()
                                .getConfig()
                                .getString("table.include.list"))
                .contains("inventory_partitioned.products")
                .contains("inventory_partitioned.products_ca");

        PostgresSourceFetchTaskContext restartedFetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);
        Pg10CaptureState restartedInitialCaptureState =
                restartedFetchTaskContext.buildInitialCaptureState(streamSplit);

        Assertions.assertThat(restartedInitialCaptureState.getChildToParentMapping())
                .isEqualTo(acceptedCaptureState.getChildToParentMapping());
        Assertions.assertThat(restartedInitialCaptureState.getParentToChildrenMapping())
                .isEqualTo(acceptedCaptureState.getParentToChildrenMapping());
    }

    @Test
    void testRestoredStreamSplitSeedsInitialCaptureStateBeforeFreshConfigMappings()
            throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        dialect.discoverDataCollections(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        StreamSplit baseStreamSplit = createMinimalStreamSplit(config, dialect);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        Map<TableId, TableId> restoredChildToParent = new LinkedHashMap<>();
        restoredChildToParent.put(childUk, parentTable);
        restoredChildToParent.put(childCa, parentTable);
        Map<TableId, List<TableId>> restoredParentToChildren = new LinkedHashMap<>();
        restoredParentToChildren.put(parentTable, List.of(childUk, childCa));

        config.setChildToParentMapping(Collections.singletonMap(childUk, parentTable));
        config.setParentToChildrenMapping(
                Collections.singletonMap(parentTable, Collections.singletonList(childUk)));
        config.setPg10PartitionMappingInitialized(true);

        StreamSplit restoredStreamSplit =
                StreamSplit.withPg10RoutingState(
                        baseStreamSplit, restoredChildToParent, restoredParentToChildren, true);
        PostgresSourceFetchTaskContext fetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);

        try {
            fetchTaskContext.configure(restoredStreamSplit);

            Pg10CaptureState initialCaptureState =
                    fetchTaskContext.buildInitialCaptureState(restoredStreamSplit);

            Assertions.assertThat(initialCaptureState.getChildToParentMapping())
                    .isEqualTo(restoredChildToParent);
            Assertions.assertThat(initialCaptureState.getParentToChildrenMapping())
                    .isEqualTo(restoredParentToChildren);
            Assertions.assertThat(config.getChildToParentMappingOrEmpty())
                    .isEqualTo(restoredChildToParent);
            Assertions.assertThat(config.getParentToChildrenMappingOrEmpty())
                    .isEqualTo(restoredParentToChildren);
        } finally {
            fetchTaskContext.close();
        }
    }

    @Test
    void testRestoreConfigureDefersPg10ReplicationConnectionUntilStreamingRuntime()
            throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        dialect.discoverDataCollections(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        StreamSplit baseStreamSplit = createMinimalStreamSplit(config, dialect);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        Map<TableId, TableId> restoredChildToParent = new LinkedHashMap<>();
        restoredChildToParent.put(childUk, parentTable);
        restoredChildToParent.put(childUs, parentTable);
        restoredChildToParent.put(childCa, parentTable);
        Map<TableId, List<TableId>> restoredParentToChildren = new LinkedHashMap<>();
        restoredParentToChildren.put(parentTable, List.of(childUk, childUs, childCa));

        StreamSplit restoredStreamSplit =
                StreamSplit.withPg10RoutingState(
                        baseStreamSplit, restoredChildToParent, restoredParentToChildren, true);
        PostgresSourceFetchTaskContext fetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);

        try {
            fetchTaskContext.configure(restoredStreamSplit);

            Assertions.assertThat(fetchTaskContext.getReplicationConnection()).isNull();

            Pg10CaptureState initialCaptureState =
                    fetchTaskContext.buildInitialCaptureState(restoredStreamSplit);

            try (Pg10StreamingSessionRuntime runtime =
                    fetchTaskContext.buildStreamingRuntime(
                            restoredStreamSplit,
                            initialCaptureState,
                            loadOffsetContext(config, 67890L, 67890L))) {
                Assertions.assertThat(runtime.getReplicationConnection()).isNotNull();
            }
        } finally {
            fetchTaskContext.close();
        }
    }

    @Test
    void testReconcileTreatsRestoredCaptureStateAsAdditiveBaseline() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        TableId childAu = new TableId(null, "inventory_partitioned", "products_au");

        List<TableId> parentTables = dialect.discoverDataCollections(config);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_au PARTITION OF inventory_partitioned.products FOR VALUES IN ('au')");
        }

        Pg10CaptureState restoredCaptureState =
                Pg10CaptureState.of(
                        new LinkedHashMap<TableId, TableId>() {
                            {
                                put(childUk, parentTable);
                                put(childUs, parentTable);
                                put(childCa, parentTable);
                            }
                        },
                        Collections.singletonMap(parentTable, List.of(childUk, childUs, childCa)));

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                    dialect.reconcilePg10PartitionMappings(
                            jdbc, restoredCaptureState, parentTables);

            Assertions.assertThat(reconcileResult.getNewChildren()).containsExactly(childAu);
            Assertions.assertThat(reconcileResult.getNewChildToParent())
                    .containsOnlyKeys(childAu)
                    .containsEntry(childAu, parentTable);
        }
    }

    @Test
    void testRestoreValidationIgnoresCompensationChildSchemasInParentBaseline() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        dialect.discoverDataCollections(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId compensationChild = new TableId(null, "inventory_partitioned", "products_ca");

        StreamSplit baseStreamSplit = createMinimalStreamSplit(config, dialect);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            PostgresSourceConfigFactory childConfigFactory =
                    getMockPostgresSourceConfigFactory(
                            inventoryPartitionedDatabase,
                            "inventory_partitioned",
                            "products_ca",
                            10);
            PostgresSourceConfig childConfig = childConfigFactory.create(0);
            PostgresDialect childDialect = new PostgresDialect(childConfig);
            io.debezium.relational.history.TableChanges.TableChange compensationSchema =
                    childDialect.queryTableSchema(jdbc, compensationChild);
            org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset
                    compensationHighWatermark =
                            org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset
                                    .of(loadOffsetContext(config, 12345L, 12345L).getOffset());

            PostgresSourceFetchTaskContext compensationContext =
                    (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);
            compensationContext.configure(baseStreamSplit);
            compensationContext.syncPg10CaptureState(
                    Pg10CaptureState.of(
                            new LinkedHashMap<TableId, TableId>() {
                                {
                                    put(childUk, parentTable);
                                    put(childUs, parentTable);
                                    put(compensationChild, parentTable);
                                }
                            },
                            Collections.singletonMap(
                                    parentTable, List.of(childUk, childUs, compensationChild))),
                    baseStreamSplit);
            SnapshotSplit compensationSnapshotSplit =
                    new SnapshotSplit(
                            compensationChild,
                            "pg10-compensation-inventory_partitioned.products_ca:0",
                            org.apache.flink.table.types.logical.RowType.of(
                                    new org.apache.flink.table.types.logical.LogicalType[] {
                                        new org.apache.flink.table.types.logical.IntType()
                                    },
                                    new String[] {"id"}),
                            null,
                            null,
                            compensationHighWatermark,
                            Collections.singletonMap(compensationChild, compensationSchema));
            FinishedSnapshotSplitInfo compensationFinishedSplitInfo =
                    compensationContext.createPg10CompensationFinishedSplitInfo(
                            compensationSnapshotSplit);
            compensationContext.close();

            StreamSplit restoredStreamSplit =
                    StreamSplit.withPg10RoutingState(
                            StreamSplit.appendCompensationSplitInfos(
                                    baseStreamSplit,
                                    Collections.singletonList(compensationFinishedSplitInfo),
                                    Collections.singletonMap(
                                            compensationChild, compensationSchema)),
                            new LinkedHashMap<TableId, TableId>() {
                                {
                                    put(childUk, parentTable);
                                    put(childUs, parentTable);
                                    put(compensationChild, parentTable);
                                }
                            },
                            Collections.singletonMap(
                                    parentTable, List.of(childUk, childUs, compensationChild)),
                            true);

            Assertions.assertThat(compensationFinishedSplitInfo.getTableId())
                    .isEqualTo(parentTable);

            Pg10CaptureState restoredCaptureState =
                    Pg10CaptureState.of(
                            restoredStreamSplit.getPg10ChildToParentMapping(),
                            restoredStreamSplit.getPg10ParentToChildrenMapping());

            PostgresSourceFetchTaskContext fetchTaskContext =
                    (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);
            try {
                fetchTaskContext.configure(restoredStreamSplit);

                Assertions.assertThatCode(
                                () ->
                                        fetchTaskContext.buildInitialCaptureState(
                                                restoredStreamSplit))
                        .doesNotThrowAnyException();
                Pg10CaptureState initialCaptureState =
                        fetchTaskContext.buildInitialCaptureState(restoredStreamSplit);
                Assertions.assertThat(initialCaptureState.getChildToParentMapping())
                        .isEqualTo(restoredCaptureState.getChildToParentMapping());
                Assertions.assertThat(initialCaptureState.getParentToChildrenMapping())
                        .isEqualTo(restoredCaptureState.getParentToChildrenMapping());
            } finally {
                fetchTaskContext.close();
            }
        }
    }

    @Test
    void testMissingRestoredChildFailsDeterministically() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        List<TableId> parentTables = dialect.discoverDataCollections(config);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
            statement.execute("DROP TABLE inventory_partitioned.products_ca");
        }

        Pg10CaptureState restoredCaptureState =
                Pg10CaptureState.of(
                        new LinkedHashMap<TableId, TableId>() {
                            {
                                put(childUk, parentTable);
                                put(childUs, parentTable);
                                put(childCa, parentTable);
                            }
                        },
                        Collections.singletonMap(parentTable, List.of(childUk, childUs, childCa)));

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Assertions.assertThatThrownBy(
                            () ->
                                    dialect.validateRestoredPg10CaptureState(
                                            jdbc, restoredCaptureState, parentTables))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining(
                            "Restored PG10 routing state references missing or altered child partitions")
                    .hasMessageContaining("inventory_partitioned.products_ca")
                    .hasMessageContaining("missing from the current catalog");
        }
    }

    @Test
    void testConfiguredRestoreEntryFailsWhenRestoredChildLeavesPublication() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_pg10_restore_missing_child";
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    "CREATE PUBLICATION "
                            + publicationName
                            + " FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us");
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("publication.name", publicationName);
        configFactory.debeziumProperties(debeziumProperties);

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childUs = new TableId(null, "inventory_partitioned", "products_us");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");

        StreamSplit baseStreamSplit = createMinimalStreamSplit(config, dialect);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        StreamSplit restoredStreamSplit =
                StreamSplit.withPg10RoutingState(
                        baseStreamSplit,
                        new LinkedHashMap<TableId, TableId>() {
                            {
                                put(childUk, parentTable);
                                put(childUs, parentTable);
                                put(childCa, parentTable);
                            }
                        },
                        Collections.singletonMap(parentTable, List.of(childUk, childUs, childCa)),
                        true);
        PostgresSourceFetchTaskContext fetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);

        try {
            fetchTaskContext.configure(restoredStreamSplit);

            Assertions.assertThatThrownBy(
                            () -> fetchTaskContext.buildInitialCaptureState(restoredStreamSplit))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining(
                            "Restored PG10 routing state references missing or altered child partitions")
                    .hasMessageContaining("inventory_partitioned.products_ca")
                    .hasMessageContaining("publication")
                    .hasMessageContaining(publicationName);
        } finally {
            fetchTaskContext.close();
        }
    }

    @Test
    void testRestartAcceptOnlyConsumesPublishedButUnacceptedChildren() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        String publicationName = "dbz_publication_pg10_restart_accept_ownership";
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP PUBLICATION IF EXISTS " + publicationName);
            statement.execute(
                    "CREATE PUBLICATION "
                            + publicationName
                            + " FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us");
        }

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("publication.name", publicationName);
        configFactory.debeziumProperties(debeziumProperties);

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        PostgresSourceFetchTaskContext fetchTaskContext =
                (PostgresSourceFetchTaskContext) dialect.createFetchTaskContext(config);

        TableId parentTable = new TableId(null, "inventory_partitioned", "products");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        TableId childAu = new TableId(null, "inventory_partitioned", "products_au");

        List<TableId> parentTables = dialect.discoverDataCollections(config);
        StreamSplit streamSplit = createMinimalStreamSplit(config, dialect);
        Pg10CaptureState initialCaptureState =
                fetchTaskContext.buildInitialCaptureState(streamSplit);

        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_au PARTITION OF inventory_partitioned.products FOR VALUES IN ('au')");
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                    dialect.reconcilePg10PartitionMappings(jdbc, initialCaptureState, parentTables);

            Assertions.assertThat(reconcileResult.getNewChildren())
                    .containsExactlyInAnyOrder(childCa, childAu);

            Pg10PublicationManager.addTablesToPublication(
                    jdbc, publicationName, Collections.singletonList(childCa));

            Pg10CaptureState acceptedCaptureState =
                    fetchTaskContext
                            .maybePrepareNextCaptureState(
                                    jdbc,
                                    dialect,
                                    initialCaptureState,
                                    parentTables,
                                    Collections.singleton(childCa))
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Expected restart accept to consume the "
                                                            + "published child"));

            fetchTaskContext.acceptPg10CaptureStateForRestart(
                    acceptedCaptureState, loadOffsetContext(config, 67890L, 67890L), streamSplit);

            Assertions.assertThat(acceptedCaptureState.getChildToParentMapping())
                    .containsEntry(childCa, parentTable)
                    .doesNotContainKey(childAu);
            Assertions.assertThat(
                            acceptedCaptureState.getParentToChildrenMapping().get(parentTable))
                    .contains(childCa)
                    .doesNotContain(childAu);
            Assertions.assertThat(config.getChildToParentMappingOrEmpty())
                    .containsEntry(childCa, parentTable)
                    .doesNotContainKey(childAu);

            Assertions.assertThat(
                            Pg10PublicationManager.findMissingPublicationChildren(
                                    jdbc, publicationName, Collections.singletonList(childAu)))
                    .containsExactly(childAu);
        }
    }

    @Test
    void testCreateFetchTaskSelectsPg10SubclassWhenPartitioned() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        // discoverDataCollections initializes the PG10 partition mappings
        dialect.discoverDataCollections(config);

        Assertions.assertThat(config.isPg10PartitionMappingInitialized()).isTrue();
        Assertions.assertThat(config.getParentToChildrenMappingOrEmpty()).isNotEmpty();

        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit mockSplit =
                createMinimalStreamSplit(config, dialect);
        org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask<?> fetchTask =
                dialect.createFetchTask(mockSplit);

        Assertions.assertThat(fetchTask)
                .isInstanceOf(
                        org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamFetchTask
                                .class);
    }

    @Test
    void testCreateFetchTaskUsesReaderInitializedPg10Mappings() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig dialectConfig = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(dialectConfig);
        PostgresSourceConfig readerConfig = configFactory.create(1);

        // Reader startup initializes PG10 mappings on its own source config instance before
        // createFetchTask() is asked to choose the stream fetch task implementation.
        dialect.discoverDataCollections(readerConfig);

        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit mockSplit =
                createMinimalStreamSplit(readerConfig, dialect);
        org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask<?> fetchTask =
                dialect.createFetchTask(mockSplit);

        Assertions.assertThat(fetchTask)
                .isInstanceOf(
                        org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamFetchTask
                                .class);
    }

    @Test
    void testRelationListenerSeesRuntimeChildBeforeAcceptance() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        dialect.discoverDataCollections(config);

        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            PostgresConnection postgresConnection = (PostgresConnection) jdbc;
            RelationAwarePostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            postgresConnection,
                            config.getDbzConnectorConfig(),
                            postgresConnection.getTypeRegistry(),
                            PostgresTopicSelector.create(config.getDbzConnectorConfig()),
                            PostgresObjectUtils.newPostgresValueConverterBuilder(
                                            config.getDbzConnectorConfig())
                                    .build(postgresConnection.getTypeRegistry()));
            AtomicReference<TableId> relationSeen = new AtomicReference<>();
            schema.setPartitionListener(relationSeen::set);

            PostgresSourceConfigFactory childConfigFactory =
                    getMockPostgresSourceConfigFactory(
                            inventoryPartitionedDatabase,
                            "inventory_partitioned",
                            "products_ca",
                            10);
            PostgresSourceConfig childConfig = childConfigFactory.create(0);
            PostgresDialect childDialect = new PostgresDialect(childConfig);

            schema.applySchemaChangesForTable(
                    1, childDialect.queryTableSchema(jdbc, childCa).getTable());

            Assertions.assertThat(relationSeen.get()).isEqualTo(childCa);
        }
    }

    @Test
    void testFilteredRuntimeChildRelationStillRegistersTableForDecoding() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactory.setIncludePartitionedTables(true);
        configFactory.startupOptions(StartupOptions.latest());

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        dialect.discoverDataCollections(config);

        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        try (Connection connection =
                        getJdbcConnection(
                                POSTGRES_CONTAINER,
                                inventoryPartitionedDatabase.getDatabaseName());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_ca PARTITION OF inventory_partitioned.products FOR VALUES IN ('ca')");
        }

        try (JdbcConnection jdbc = dialect.openJdbcConnection(config)) {
            PostgresConnection postgresConnection = (PostgresConnection) jdbc;
            RelationAwarePostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            postgresConnection,
                            config.getDbzConnectorConfig(),
                            postgresConnection.getTypeRegistry(),
                            PostgresTopicSelector.create(config.getDbzConnectorConfig()),
                            PostgresObjectUtils.newPostgresValueConverterBuilder(
                                            config.getDbzConnectorConfig())
                                    .build(postgresConnection.getTypeRegistry()));

            PostgresSourceConfigFactory childConfigFactory =
                    getMockPostgresSourceConfigFactory(
                            inventoryPartitionedDatabase,
                            "inventory_partitioned",
                            "products_ca",
                            10);
            PostgresSourceConfig childConfig = childConfigFactory.create(0);
            PostgresDialect childDialect = new PostgresDialect(childConfig);

            schema.applySchemaChangesForTable(
                    1, childDialect.queryTableSchema(jdbc, childCa).getTable());

            Assertions.assertThat(schema.tableFor(1)).isNotNull();
            Assertions.assertThat(schema.tableFor(1).id()).isEqualTo(childCa);
        }
    }

    @Test
    void testCreateFetchTaskSelectsBaseClassWhenNotPartitioned() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTable(inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        // includePartitionedTables defaults to false
        configFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);

        // discoverDataCollections does NOT initialize PG10 mappings
        dialect.discoverDataCollections(config);

        Assertions.assertThat(config.isPg10PartitionMappingInitialized()).isFalse();

        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit mockSplit =
                createMinimalStreamSplit(config, dialect);
        org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask<?> fetchTask =
                dialect.createFetchTask(mockSplit);

        Assertions.assertThat(fetchTask)
                .isInstanceOf(
                        org.apache.flink.cdc.connectors.postgres.source.fetch
                                .PostgresStreamFetchTask.class)
                .isNotInstanceOf(
                        org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamFetchTask
                                .class);
    }

    private org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit
            createMinimalStreamSplit(PostgresSourceConfig config, PostgresDialect dialect)
                    throws Exception {
        org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase>
                enumCtx =
                        new org.apache.flink.api.connector.source.mocks
                                .MockSplitEnumeratorContext<>(1);
        org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner assigner =
                new org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner(
                        config,
                        dialect,
                        new org.apache.flink.cdc.connectors.postgres.source.offset
                                .PostgresOffsetFactory(),
                        enumCtx);
        assigner.open();

        Map<TableId, io.debezium.relational.history.TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(config);
        return org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.fillTableSchemas(
                assigner.createStreamSplit(), tableSchemas);
    }

    private PostgresOffsetContext loadOffsetContext(
            PostgresSourceConfig config, Long lsn, Long lastCommitLsn) {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, lsn);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, lastCommitLsn);
        return new PostgresOffsetContext.Loader(config.getDbzConnectorConfig()).load(offsetValues);
    }

    private UniqueDatabase createInventoryPartitionedDatabase() {
        return new UniqueDatabase(
                POSTGRES_CONTAINER,
                "postgres_pg10",
                "inventory_partitioned",
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
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
            statement.execute("DROP SCHEMA IF EXISTS inventory_partitioned CASCADE");
            statement.execute("CREATE SCHEMA inventory_partitioned");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products ("
                            + " id SERIAL NOT NULL,"
                            + " name VARCHAR(255) NOT NULL DEFAULT 'flink',"
                            + " description VARCHAR(512),"
                            + " weight FLOAT,"
                            + " country VARCHAR(20) NOT NULL"
                            + ") PARTITION BY LIST(country)");
            statement.execute(
                    "ALTER SEQUENCE inventory_partitioned.products_id_seq RESTART WITH 101");
            statement.execute("ALTER TABLE inventory_partitioned.products REPLICA IDENTITY FULL");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_uk PARTITION OF inventory_partitioned.products FOR VALUES IN ('uk')");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_us PARTITION OF inventory_partitioned.products FOR VALUES IN ('us')");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES "
                            + "(default,'scooter','Small 2-wheel scooter',3.14, 'us'),"
                            + "(default,'car battery','12V car battery',8.1, 'us'),"
                            + "(default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8, 'us'),"
                            + "(default,'hammer','12oz carpenter''s hammer',0.75, 'us'),"
                            + "(default,'hammer','14oz carpenter''s hammer',0.875, 'us'),"
                            + "(default,'hammer','16oz carpenter''s hammer',1.0, 'uk'),"
                            + "(default,'rocks','box of assorted rocks',5.3, 'uk'),"
                            + "(default,'jacket','water resistent black wind breaker',0.1, 'uk'),"
                            + "(default,'spare tire','24 inch spare tire',22.2, 'uk')");
        }
    }
}
