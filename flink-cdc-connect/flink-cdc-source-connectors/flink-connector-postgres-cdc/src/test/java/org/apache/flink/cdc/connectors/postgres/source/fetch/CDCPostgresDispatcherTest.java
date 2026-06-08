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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Unit tests for {@link CDCPostgresDispatcher}. */
class CDCPostgresDispatcherTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD = new TableId(null, "public", "orders_2024");
    private static final TableId EXISTING_CHILD = new TableId(null, "public", "orders_2023");
    private static final TableId ORDINARY_TABLE = new TableId(null, "public", "customers");

    @Test
    void preRouteTableIdRefreshesMissingChildSchemaBeforeRegisteringParent() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicBoolean loaderCalled = new AtomicBoolean(false);

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.singletonMap(CHILD, PARENT),
                        tableId -> {
                            loaderCalled.set(true);
                            Assertions.assertThat(tableId).isEqualTo(CHILD);
                            schema.addRuntimeTable(childTable);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loaded();
                        });

        Assertions.assertThat(schema.tableFor(CHILD)).isNull();

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(PARENT);
        Assertions.assertThat(loaderCalled).isTrue();
        Assertions.assertThat(schema.tableFor(PARENT)).isNotNull();
        Assertions.assertThat(schema.tableFor(PARENT).primaryKeyColumnNames())
                .containsExactly("id", "bucket");
    }

    @Test
    void preRouteTableIdDiscoversUnknownChildPartitionBeforeEmitterConstruction() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicReference<TableId> listenerChild = new AtomicReference<>();
        AtomicReference<TableId> listenerParent = new AtomicReference<>();

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.singletonMap(EXISTING_CHILD, PARENT),
                        tableId -> {
                            Assertions.assertThat(tableId).isEqualTo(CHILD);
                            schema.addRuntimeTable(childTable);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        },
                        (childId, parentId) -> {
                            listenerChild.set(childId);
                            listenerParent.set(parentId);
                        });

        Assertions.assertThat(schema.tableFor(CHILD)).isNull();

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(PARENT);
        Assertions.assertThat(listenerChild).hasValue(CHILD);
        Assertions.assertThat(listenerParent).hasValue(PARENT);
        Assertions.assertThat(schema.tableFor(CHILD)).isNotNull();
        Assertions.assertThat(schema.tableFor(PARENT)).isNotNull();
    }

    @Test
    void preRouteTableIdDiscoversUnknownChildPartitionWhenInitialRouteMapIsEmpty() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicReference<TableId> listenerChild = new AtomicReference<>();
        AtomicReference<TableId> listenerParent = new AtomicReference<>();

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.emptyMap(),
                        tableId -> {
                            Assertions.assertThat(tableId).isEqualTo(CHILD);
                            schema.addRuntimeTable(childTable);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        },
                        (childId, parentId) -> {
                            listenerChild.set(childId);
                            listenerParent.set(parentId);
                        });

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(PARENT);
        Assertions.assertThat(listenerChild).hasValue(CHILD);
        Assertions.assertThat(listenerParent).hasValue(PARENT);
        Assertions.assertThat(schema.tableFor(routed))
                .as("exact TableId passed to PostgresChangeRecordEmitter must have table metadata")
                .isNotNull();
    }

    @Test
    void preRouteTableIdResolvesRegisteredUnknownChildPartitionBeforeEmitterConstruction() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicBoolean loaderCalled = new AtomicBoolean(false);

        schema.addRuntimeTable(childTable);
        Assertions.assertThat(schema.tableFor(CHILD)).isNotNull();

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.singletonMap(EXISTING_CHILD, PARENT),
                        tableId -> {
                            loaderCalled.set(true);
                            Assertions.assertThat(tableId).isEqualTo(CHILD);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        });

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(PARENT);
        Assertions.assertThat(loaderCalled).isTrue();
        Assertions.assertThat(schema.tableFor(PARENT)).isNotNull();
    }

    @Test
    void preRouteTableIdCachesOrdinaryIncludedTableAfterOneLazyLookup() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.customers")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        AtomicInteger loaderCalls = new AtomicInteger();

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.singletonMap(CHILD, PARENT),
                        tableId -> {
                            loaderCalls.incrementAndGet();
                            Assertions.assertThat(tableId).isEqualTo(ORDINARY_TABLE);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.notLoaded();
                        });

        TableId routed = dispatcher.preRouteTableId(ORDINARY_TABLE);
        TableId routedAgain = dispatcher.preRouteTableId(ORDINARY_TABLE);

        Assertions.assertThat(routed).isEqualTo(ORDINARY_TABLE);
        Assertions.assertThat(routedAgain).isEqualTo(ORDINARY_TABLE);
        Assertions.assertThat(loaderCalls).hasValue(1);
    }

    @Test
    void preRouteTableIdSkipsLazyLookupWhenPartitionRoutingDisabled() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig().with("table.include.list", "public.*").build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        schema.setPartitionRoutingEnabled(false);
        AtomicBoolean loaderCalled = new AtomicBoolean(false);

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.emptyMap(),
                        tableId -> {
                            loaderCalled.set(true);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        });

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(CHILD);
        Assertions.assertThat(loaderCalled).isFalse();
    }

    @Test
    void preRouteTableIdDiscoversUnknownChildPartitionEvenWhenChildMatchesFilter() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig().with("table.include.list", "public.*").build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicReference<TableId> listenerChild = new AtomicReference<>();
        AtomicReference<TableId> listenerParent = new AtomicReference<>();

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.emptyMap(),
                        tableId -> {
                            Assertions.assertThat(tableId).isEqualTo(CHILD);
                            schema.addRuntimeTable(childTable);
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        },
                        (childId, parentId) -> {
                            listenerChild.set(childId);
                            listenerParent.set(parentId);
                        });

        TableId routed = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(routed).isEqualTo(PARENT);
        Assertions.assertThat(listenerChild).hasValue(CHILD);
        Assertions.assertThat(listenerParent).hasValue(PARENT);
        Assertions.assertThat(schema.tableFor(PARENT)).isNotNull();
    }

    @Test
    void preRouteTableIdRebuildsCachedParentSchemaWhenActiveRegistryLostIt() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        TestHelper.defaultConfig()
                                .with("table.include.list", "public.orders")
                                .build());
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(config);
        TestPostgresSchema schema = new TestPostgresSchema(config, topicSelector);
        Table childTable = childTable();
        AtomicBoolean loaderCalledAfterRegistryLoss = new AtomicBoolean(false);

        CDCPostgresDispatcher dispatcher =
                new CDCPostgresDispatcher(
                        config,
                        topicSelector,
                        schema,
                        queue(config),
                        config.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        PostgresObjectUtils.newEventMetadataProvider(),
                        heartbeatFactory(config, topicSelector),
                        SchemaNameAdjuster.create(),
                        Collections.singletonMap(CHILD, PARENT),
                        tableId -> {
                            if (schema.tableFor(CHILD) == null) {
                                schema.addRuntimeTable(childTable);
                            } else {
                                loaderCalledAfterRegistryLoss.set(true);
                            }
                            return CDCPostgresDispatcher.TableSchemaLoadResult.loadedAsChildOf(
                                    PARENT);
                        });

        TableId firstRouted = dispatcher.preRouteTableId(CHILD);
        Assertions.assertThat(firstRouted).isEqualTo(PARENT);
        Assertions.assertThat(schema.tableFor(PARENT)).isNotNull();

        // Simulate a split-schema/session reconstruction where the dispatcher's cache survives but
        // the active Debezium Tables registry no longer contains the synthesized parent metadata.
        schema.removeRuntimeTable(PARENT);
        Assertions.assertThat(schema.tableFor(PARENT)).isNull();

        TableId secondRouted = dispatcher.preRouteTableId(CHILD);

        Assertions.assertThat(secondRouted).isEqualTo(PARENT);
        Assertions.assertThat(loaderCalledAfterRegistryLoss).isFalse();
        Assertions.assertThat(schema.tableFor(secondRouted))
                .as("exact TableId passed to PostgresChangeRecordEmitter must have table metadata")
                .isNotNull();
    }

    private static ChangeEventQueue<DataChangeEvent> queue(PostgresConnectorConfig config) {
        return new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(config.getPollInterval())
                .maxBatchSize(config.getMaxBatchSize())
                .maxQueueSize(config.getMaxQueueSize())
                .maxQueueSizeInBytes(config.getMaxQueueSizeInBytes())
                .build();
    }

    private static HeartbeatFactory<TableId> heartbeatFactory(
            PostgresConnectorConfig config, TopicSelector<TableId> topicSelector) {
        return new HeartbeatFactory<>(
                config,
                topicSelector,
                SchemaNameAdjuster.create(),
                () ->
                        new PostgresConnection(
                                config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL),
                exception -> {});
    }

    private static Table childTable() {
        return Table.editor()
                .tableId(CHILD)
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("INT4")
                                .jdbcType(java.sql.Types.INTEGER)
                                .position(1)
                                .optional(false)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("bucket")
                                .type("VARCHAR")
                                .jdbcType(java.sql.Types.VARCHAR)
                                .position(2)
                                .optional(false)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("payload")
                                .type("VARCHAR")
                                .jdbcType(java.sql.Types.VARCHAR)
                                .position(3)
                                .optional(true)
                                .create())
                .setPrimaryKeyNames("id", "bucket")
                .create();
    }

    private static class TestPostgresSchema extends RelationAwarePostgresSchema {

        TestPostgresSchema(PostgresConnectorConfig config, TopicSelector<TableId> topicSelector) {
            super(config, null, null, topicSelector, null);
            setPartitionRoutingEnabled(true);
        }

        void addRuntimeTable(Table table) {
            applySchemaChangesForTable(1, table);
        }

        void removeRuntimeTable(TableId tableId) {
            tables().removeTable(tableId);
        }

        @Override
        public void buildAndRegisterSchema(Table table) {
            tables().overwriteTable(table);
        }
    }
}
