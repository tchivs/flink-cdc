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

import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionRoutingState}. */
class PartitionRoutingStateTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");

    @Test
    void routesChildPartitionsToParent() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThat(routingState.containsParent(PARENT)).isTrue();
        assertThat(routingState.containsChild(CHILD_2024)).isTrue();
        assertThat(routingState.routeToLogicalTable(CHILD_2024)).isEqualTo(PARENT);
        assertThat(routingState.routeToLogicalTable(PARENT)).isEqualTo(PARENT);
        assertThat(routingState.allChildren()).containsExactly(CHILD_2024, CHILD_2025);
    }

    @Test
    void rejectsAmbiguousChildMapping() {
        TableId otherParent = new TableId(null, "public", "orders_archive");
        Map<TableId, java.util.List<TableId>> mappings = new LinkedHashMap<>();
        mappings.put(PARENT, Collections.singletonList(CHILD_2024));
        mappings.put(otherParent, Collections.singletonList(CHILD_2024));

        assertThatThrownBy(() -> PartitionRoutingState.of(mappings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("belongs to multiple parents");
    }

    @Test
    void snapshotsMappingsAndExposesImmutableViews() {
        List<TableId> children = new ArrayList<>(Collections.singletonList(CHILD_2024));
        Map<TableId, List<TableId>> mappings = new LinkedHashMap<>();
        mappings.put(PARENT, children);

        PartitionRoutingState routingState = PartitionRoutingState.of(mappings);
        children.add(CHILD_2025);

        assertThat(routingState.childrenOf(PARENT)).containsExactly(CHILD_2024);
        assertThatThrownBy(() -> routingState.childrenOf(PARENT).add(CHILD_2025))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> routingState.parentToChildren().put(PARENT, children))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> routingState.allChildren().add(CHILD_2025))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void mergesNewChildMappingsWithoutMutatingOriginalSnapshot() {
        PartitionRoutingState original =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD_2024)));

        PartitionRoutingState merged =
                original.merge(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD_2025)));

        assertThat(original.childrenOf(PARENT)).containsExactly(CHILD_2024);
        assertThat(merged.childrenOf(PARENT)).containsExactly(CHILD_2024, CHILD_2025);
    }

    @Test
    void detectsMixedParentAndChildCapture() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        assertThat(routingState.findMixedParentChildCaptures(Arrays.asList(PARENT, CHILD_2024)))
                .containsExactlyInAnyOrder(PARENT, CHILD_2024);
        assertThat(routingState.findMixedParentChildCaptures(Collections.singletonList(CHILD_2024)))
                .isEmpty();
    }

    @Test
    void rewritesSourceStructToLogicalParentTable() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));
        PostgresTableIdRouter router = PostgresTableIdRouter.of(() -> routingState);
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema = SchemaBuilder.struct().field(SOURCE, sourceSchema).build();
        Struct source =
                new Struct(sourceSchema)
                        .put(SCHEMA_NAME_KEY, CHILD_2024.schema())
                        .put(TABLE_NAME_KEY, CHILD_2024.table());
        Struct value = new Struct(valueSchema).put(SOURCE, source);

        router.rewriteSourceStruct(value);

        assertThat(source.getString(SCHEMA_NAME_KEY)).isEqualTo(PARENT.schema());
        assertThat(source.getString(TABLE_NAME_KEY)).isEqualTo(PARENT.table());
    }

    @Test
    void rejectsUnknownRuntimeChildForDecoderbufs() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD_2024)));
        PostgresTableIdRouter router = PostgresTableIdRouter.of(() -> routingState);
        TableId runtimeChild = new TableId(null, "public", "orders_2026");

        assertThatThrownBy(
                        () ->
                                router.validateKnownChildForDecoderbufs(
                                        runtimeChild,
                                        "decoderbufs",
                                        StartupOptions.latest(),
                                        PARENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PostgresTableIdRouter.ERR_PR_001)
                .hasMessageContaining("decoding.plugin.name=decoderbufs")
                .hasMessageContaining("scan.startup.mode=LATEST_OFFSET")
                .hasMessageContaining(PARENT.toString())
                .hasMessageContaining(runtimeChild.toString());
    }

    @Test
    void acceptsSeededChildOrPgoutputRuntimeChild() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Collections.singletonList(CHILD_2024)));
        PostgresTableIdRouter router = PostgresTableIdRouter.of(() -> routingState);

        router.validateKnownChildForDecoderbufs(
                CHILD_2024, "decoderbufs", StartupOptions.latest(), PARENT);
        router.validateKnownChildForDecoderbufs(
                new TableId(null, "public", "orders_2026"),
                "pgoutput",
                StartupOptions.latest(),
                PARENT);
        router.validateKnownChildForDecoderbufs(
                new TableId(null, "public", "customers"),
                "decoderbufs",
                StartupOptions.latest(),
                null);
    }
}
