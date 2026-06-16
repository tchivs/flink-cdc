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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionPublicationRefresher}. */
class PartitionPublicationRefresherTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");
    private static final TableId ORDINARY = new TableId(null, "public", "customers");

    @Test
    void expandsParentPublicationMemberToChildrenOnly() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThat(
                        PartitionPublicationRefresher.computeDesiredMembers(
                                Arrays.asList(ORDINARY, PARENT), routingState))
                .containsExactly(ORDINARY, CHILD_2024, CHILD_2025);
    }

    @Test
    void rejectsMixedParentAndChildPublicationMembers() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.computeDesiredMembers(
                                        Arrays.asList(PARENT, CHILD_2024), routingState))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_004)
                .hasMessageContaining(PARENT.toString())
                .hasMessageContaining(CHILD_2024.toString());
    }

    @Test
    void keepsExplicitChildMembersWhenParentIsNotRequested() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThat(
                        PartitionPublicationRefresher.computeDesiredMembers(
                                Arrays.asList(CHILD_2024, ORDINARY), routingState))
                .containsExactly(CHILD_2024, ORDINARY);
    }

    @Test
    void rejectsDecoderbufsWithPublishViaPartitionRoot() {
        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher
                                        .validateDecoderbufsPublishViaPartitionRoot(
                                                "decoderbufs", true, "cdc_pub"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_005)
                .hasMessageContaining("decoderbufs")
                .hasMessageContaining("publish_via_partition_root=true")
                .hasMessageContaining("pgoutput");
    }

    @Test
    void acceptsPgoutputOrDisabledPublishViaPartitionRoot() {
        PartitionPublicationRefresher.validateDecoderbufsPublishViaPartitionRoot(
                "pgoutput", true, "cdc_pub");
        PartitionPublicationRefresher.validateDecoderbufsPublishViaPartitionRoot(
                "decoderbufs", false, "cdc_pub");
    }

    @Test
    void acceptsDecoderbufsStaticRoutingWithDefaultPublicationMode() {
        PartitionPublicationRefresher.validatePartitionRoutingSupport(
                sourceConfig("decoderbufs", null), routingState(), false);
    }

    @Test
    void rejectsDecoderbufsWithPublicationSpecificPartitionMode() {
        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.validatePartitionRoutingSupport(
                                        sourceConfig("decoderbufs", "filtered"),
                                        routingState(),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_006)
                .hasMessageContaining("decoding.plugin.name=decoderbufs")
                .hasMessageContaining("debezium.publication.autocreate.mode=filtered")
                .hasMessageContaining(PARENT.toString())
                .hasMessageContaining("Remediation");
    }

    @Test
    void rejectsRefreshForUnsupportedCombinations() {
        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.validatePartitionRoutingSupport(
                                        sourceConfig("decoderbufs", null, true),
                                        routingState(),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_007)
                .hasMessageContaining("scan.partition.publication.refresh.enabled=true")
                .hasMessageContaining("decoding.plugin.name=decoderbufs");

        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.validatePartitionRoutingSupport(
                                        sourceConfig("pgoutput", "all_tables", true),
                                        routingState(),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_007)
                .hasMessageContaining("debezium.publication.autocreate.mode=all_tables");

        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.validatePartitionRoutingSupport(
                                        sourceConfig(
                                                "pgoutput",
                                                "disabled",
                                                true,
                                                StartupOptions.snapshot()),
                                        routingState(),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_007)
                .hasMessageContaining("scan.startup.mode=SNAPSHOT");
    }

    @Test
    void acceptsPgoutputDisabledModeWithExplicitRefresh() {
        PartitionPublicationRefresher.validatePartitionRoutingSupport(
                sourceConfig("pgoutput", "disabled", true), routingState(), false);
    }

    @Test
    void acceptsPgoutputFilteredModeWithExplicitRefresh() {
        PartitionPublicationRefresher.validatePartitionRoutingSupport(
                sourceConfig("pgoutput", "filtered", true), routingState(), false);
    }

    @Test
    void refreshPublicationForExplicitDisabledOrFilteredMode() {
        assertThat(
                        PartitionPublicationRefresher.shouldRefreshPublication(
                                sourceConfig("pgoutput", "disabled", true)))
                .isTrue();
        assertThat(
                        PartitionPublicationRefresher.shouldRefreshPublication(
                                sourceConfig("pgoutput", "filtered", true)))
                .isTrue();
        assertThat(
                        PartitionPublicationRefresher.shouldRefreshPublication(
                                sourceConfig("pgoutput", "disabled", false)))
                .isFalse();
        assertThat(
                        PartitionPublicationRefresher.shouldRefreshPublication(
                                sourceConfig("pgoutput", null, true)))
                .isFalse();
    }

    @Test
    void buildsIdempotentAddTablesStatement() {
        assertThat(
                        PartitionPublicationRefresher.buildAddTablesStatement(
                                "cdc_pub", Arrays.asList(CHILD_2024, CHILD_2025)))
                .isEqualTo(
                        "ALTER PUBLICATION \"cdc_pub\" ADD TABLE \"public\".\"orders_2024\", \"public\".\"orders_2025\";");
    }

    @Test
    void acceptsPgoutputFilteredModeWhenParentsResolveToChildrenOnly() {
        PartitionPublicationRefresher.validatePartitionRoutingSupport(
                sourceConfig("pgoutput", "filtered"), routingState(), false);
    }

    @Test
    void keepsDecoderbufsPublishViaPartitionRootFailureInCentralValidator() {
        assertThatThrownBy(
                        () ->
                                PartitionPublicationRefresher.validatePartitionRoutingSupport(
                                        sourceConfig("decoderbufs", null), routingState(), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartitionPublicationRefresher.ERR_PR_005)
                .hasMessageContaining("publish_via_partition_root=true");
    }

    private static PartitionRoutingState routingState() {
        return PartitionRoutingState.of(
                Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));
    }

    private static PostgresSourceConfig sourceConfig(String decoderName, String publicationMode) {
        return sourceConfig(decoderName, publicationMode, false);
    }

    private static PostgresSourceConfig sourceConfig(
            String decoderName, String publicationMode, boolean publicationRefreshEnabled) {
        return sourceConfig(
                decoderName, publicationMode, publicationRefreshEnabled, StartupOptions.initial());
    }

    private static PostgresSourceConfig sourceConfig(
            String decoderName,
            String publicationMode,
            boolean publicationRefreshEnabled,
            StartupOptions startupOptions) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList("public.orders");
        configFactory.decodingPluginName(decoderName);
        configFactory.startupOptions(startupOptions);
        configFactory.setIncludePartitionedTables(true);
        configFactory.setPartitionPublicationRefreshEnabled(publicationRefreshEnabled);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(PostgresConnectorConfig.PUBLICATION_NAME.name(), "cdc_pub");
        if (publicationMode != null) {
            dbzProperties.setProperty(
                    PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(), publicationMode);
        }
        configFactory.debeziumProperties(dbzProperties);
        return configFactory.create(0);
    }
}
