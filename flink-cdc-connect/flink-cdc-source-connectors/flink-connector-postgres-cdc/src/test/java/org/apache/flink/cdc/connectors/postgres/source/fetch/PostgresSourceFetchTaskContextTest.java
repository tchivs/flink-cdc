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

import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionAwareTableFilter;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionRoutingState;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PostgresSourceFetchTaskContext}. */
class PostgresSourceFetchTaskContextTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD = new TableId(null, "public", "orders_2025");
    private static final TableId RUNTIME_CHILD = new TableId(null, "public", "orders_2026");

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader<PostgresOffsetContext> offsetLoader;

    @BeforeEach
    public void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    void shouldNotResetLsnWhenLastCommitLsnIsNull() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);
        Assertions.assertThat(lastKnownLsn(offsetContext)).isEqualTo(Lsn.valueOf(12345L));
    }

    @Test
    void installsSharedPartitionAwareFilterBeforeRuntimeObjectsAreCreated() {
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        dialect.compareAndSetRoutingState(
                PartitionRoutingState.EMPTY,
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD))));

        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        PostgresConnectorConfig partitionAwareConfig =
                context.installPartitionAwareConfig(sourceConfig.getDbzConnectorConfig());

        assertThat(sourceConfig.getTableFilters().dataCollectionFilter().isIncluded(CHILD))
                .isFalse();
        assertThat(context.getTableFilter()).isInstanceOf(PartitionAwareTableFilter.class);
        assertThat(partitionAwareConfig.getTableFilters().dataCollectionFilter())
                .isSameAs(context.getTableFilter());
        assertThat(context.getDbzConnectorConfig().getTableFilters().dataCollectionFilter())
                .isSameAs(context.getTableFilter());
        assertThat(context.getTableFilter().isIncluded(CHILD)).isTrue();
        assertThat(context.getTableIdRouter().route(CHILD)).isEqualTo(PARENT);
    }

    @Test
    void mergesPgoutputRuntimeChildBeforeRelationFilterUsesRoutingState() {
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        dialect.compareAndSetRoutingState(
                PartitionRoutingState.EMPTY,
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD))));
        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        context.installPartitionAwareConfig(sourceConfig.getDbzConnectorConfig());

        assertThat(context.getTableFilter().isIncluded(RUNTIME_CHILD)).isFalse();

        context.mergePgoutputRuntimeChild(RUNTIME_CHILD, PARENT);

        assertThat(dialect.routingState().containsChild(RUNTIME_CHILD)).isTrue();
        assertThat(dialect.routingState().routeToLogicalTable(RUNTIME_CHILD)).isEqualTo(PARENT);
        assertThat(context.getTableFilter().isIncluded(RUNTIME_CHILD)).isTrue();
        assertThat(context.getTableIdRouter().route(RUNTIME_CHILD)).isEqualTo(PARENT);
    }

    @Test
    void doesNotRefreshPublicationForFilteredModeUnlessRefreshIsExplicitlyEnabled() {
        PostgresSourceConfig sourceConfig = sourceConfig("filtered", false);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        dialect.compareAndSetRoutingState(
                PartitionRoutingState.EMPTY,
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD))));
        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        context.installPartitionAwareConfig(sourceConfig.getDbzConnectorConfig());

        context.mergePgoutputRuntimeChild(RUNTIME_CHILD, PARENT);

        assertThat(dialect.routingState().containsChild(RUNTIME_CHILD)).isTrue();
        assertThat(context.getTableFilter().isIncluded(RUNTIME_CHILD)).isTrue();
    }

    @Test
    void ignoresRuntimeChildWhenParentIsNotCaptured() {
        PostgresSourceConfig sourceConfig = sourceConfig();
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        dialect.compareAndSetRoutingState(
                PartitionRoutingState.EMPTY,
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD))));
        PostgresSourceFetchTaskContext context =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        context.installPartitionAwareConfig(sourceConfig.getDbzConnectorConfig());

        context.mergePgoutputRuntimeChild(
                new TableId(null, "public", "customers_2026"),
                new TableId(null, "public", "customers"));

        assertThat(dialect.routingState().allChildren()).containsExactly(CHILD);
    }

    @Test
    void sourceConfigCarriesPublicationRefreshOption() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList("public.orders");
        configFactory.setPartitionPublicationRefreshEnabled(true);

        assertThat(configFactory.create(0).partitionPublicationRefreshEnabled()).isTrue();
    }

    private static PostgresSourceConfig sourceConfig() {
        return sourceConfig(null, false);
    }

    private static PostgresSourceConfig sourceConfig(
            String publicationMode, boolean publicationRefreshEnabled) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList("public.orders");
        configFactory.decodingPluginName("pgoutput");
        configFactory.setIncludePartitionedTables(true);
        configFactory.setPartitionPublicationRefreshEnabled(publicationRefreshEnabled);
        if (publicationMode != null) {
            Properties dbzProperties = new Properties();
            dbzProperties.setProperty(PostgresConnectorConfig.PUBLICATION_NAME.name(), "cdc_pub");
            dbzProperties.setProperty(
                    PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(), publicationMode);
            configFactory.debeziumProperties(dbzProperties);
        }
        return configFactory.create(0);
    }
}
