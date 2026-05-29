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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link PartitionMapper} pure-logic helpers. */
class PartitionMapperTest {

    @Test
    void parseTableIdHandlesUnquotedSchemaQualified() {
        TableId id = PartitionMapper.parseTableId("public.products");
        Assertions.assertThat(id.schema()).isEqualTo("public");
        Assertions.assertThat(id.table()).isEqualTo("products");
        Assertions.assertThat(id.catalog()).isNull();
    }

    @Test
    void parseTableIdAddsPublicWhenSchemaMissing() {
        TableId id = PartitionMapper.parseTableId("orders");
        Assertions.assertThat(id.schema()).isEqualTo("public");
        Assertions.assertThat(id.table()).isEqualTo("orders");
    }

    @Test
    void parseTableIdStripsDoubleQuotes() {
        TableId id = PartitionMapper.parseTableId("\"My Schema\".\"products\"");
        Assertions.assertThat(id.schema()).isEqualTo("My Schema");
        Assertions.assertThat(id.table()).isEqualTo("products");
    }

    @Test
    void parseTableIdMisplitsLiteralDotInsideQuotedTable() {
        // KNOWN LIMITATION: lastIndexOf('.') splits inside the quoted table name when the
        // table identifier itself contains a literal dot. Documented here so future fixes
        // (quote-aware split) can flip this assertion to the correct expectation.
        TableId id = PartitionMapper.parseTableId("\"My Schema\".\"My.Table\"");
        // schema portion ends up as the raw substring before the LAST dot, including
        // the leading bytes of the table identifier.
        Assertions.assertThat(id.schema()).isEqualTo("\"My Schema\".\"My");
        Assertions.assertThat(id.table()).isEqualTo("Table\"");
    }

    @Test
    void parseTableIdUnescapesEmbeddedDoubleQuote() {
        // PG quotes embedded `"` as `""`
        TableId id = PartitionMapper.parseTableId("\"sch\".\"t\"\"name\"");
        Assertions.assertThat(id.schema()).isEqualTo("sch");
        Assertions.assertThat(id.table()).isEqualTo("t\"name");
    }

    @Test
    void buildChildToParentMappingFlattensReverseIndex() {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        TableId parentA = new TableId(null, "public", "products");
        TableId childA1 = new TableId(null, "public", "products_us");
        TableId childA2 = new TableId(null, "public", "products_uk");
        TableId parentB = new TableId(null, "public", "orders");
        TableId childB1 = new TableId(null, "public", "orders_2024");
        parentToChildren.put(parentA, Arrays.asList(childA1, childA2));
        parentToChildren.put(parentB, Collections.singletonList(childB1));

        Map<TableId, TableId> childToParent =
                PartitionMapper.buildChildToParentMapping(parentToChildren);

        Assertions.assertThat(childToParent)
                .hasSize(3)
                .containsEntry(childA1, parentA)
                .containsEntry(childA2, parentA)
                .containsEntry(childB1, parentB);
    }

    @Test
    void buildChildToParentMappingHandlesEmpty() {
        Assertions.assertThat(PartitionMapper.buildChildToParentMapping(new HashMap<>())).isEmpty();
    }

    @Test
    void getAllChildTableIdsFlattensValues() {
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        parentToChildren.put(
                new TableId(null, "public", "products"),
                Arrays.asList(
                        new TableId(null, "public", "products_us"),
                        new TableId(null, "public", "products_uk")));
        parentToChildren.put(
                new TableId(null, "public", "orders"),
                new ArrayList<>(
                        Collections.singletonList(new TableId(null, "public", "orders_2024"))));

        List<TableId> all = PartitionMapper.getAllChildTableIds(parentToChildren);
        Assertions.assertThat(all)
                .hasSize(3)
                .extracting(TableId::table)
                .containsExactlyInAnyOrder("products_us", "products_uk", "orders_2024");
    }

    @Test
    void getAllChildTableIdsEmpty() {
        Assertions.assertThat(PartitionMapper.getAllChildTableIds(new HashMap<>())).isEmpty();
    }
}
