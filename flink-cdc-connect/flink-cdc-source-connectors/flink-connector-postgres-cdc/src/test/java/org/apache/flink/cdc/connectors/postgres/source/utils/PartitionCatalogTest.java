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

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionCatalog}. */
class PartitionCatalogTest {

    private static final TableId PARENT = new TableId(null, "public", "orders");
    private static final TableId CHILD_2024 = new TableId(null, "public", "orders_2024");
    private static final TableId CHILD_2025 = new TableId(null, "public", "orders_2025");
    private static final TableId ORDINARY = new TableId(null, "public", "customers");

    @Test
    void expandsCapturedParentsToChildrenAndKeepsOrdinaryTables() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        List<TableId> expanded =
                PartitionCatalog.expandParentsToChildren(
                        Arrays.asList(ORDINARY, PARENT), routingState);

        assertThat(expanded).containsExactly(ORDINARY, CHILD_2024, CHILD_2025);
    }

    @Test
    void leavesCapturedChildrenUnchangedWhenParentIsNotCaptured() {
        PartitionRoutingState routingState =
                PartitionRoutingState.of(
                        Collections.singletonMap(PARENT, Arrays.asList(CHILD_2024, CHILD_2025)));

        List<TableId> expanded =
                PartitionCatalog.expandParentsToChildren(
                        Arrays.asList(CHILD_2024, ORDINARY), routingState);

        assertThat(expanded).containsExactly(CHILD_2024, ORDINARY);
    }

    @Test
    void quotesRegclassLiteralWithSchemaAndEscapedIdentifierQuotes() {
        TableId quoted = new TableId(null, "Mixed Schema", "orders\"2025");

        assertThat(PartitionCatalog.toRegclassLiteral(quoted))
                .isEqualTo("'\"Mixed Schema\".\"orders\"\"2025\"'::regclass");
    }

    @Test
    void quotesRegclassLiteralWithoutSchema() {
        TableId noSchema = new TableId(null, null, "orders");

        assertThat(PartitionCatalog.toRegclassLiteral(noSchema))
                .isEqualTo("'\"orders\"'::regclass");
    }
}
