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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgresPartitionRouter}.
 *
 * <p>This test class focuses on verifying partition routing logic, particularly the behavior needed
 * for PostgreSQL 10's partition tables where child partitions must be routed to their parent
 * tables.
 */
class PostgresPartitionRouterTest {

    // =====================================================================================
    // Test: Basic Partition Routing with Pattern Matching
    // =====================================================================================

    @Test
    void testBasicPartitionRouting_WithColonFormat() {
        // Test the "parent:childPattern" format introduced for PG10 support
        String tables = "public.orders,public.products_by_category";
        String partitionTables =
                "public.orders:public\\.orders_\\d+_q\\d+,public.products_by_category:public\\.products_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Test child partition routing to parent
        TableId childOrder = new TableId(null, "public", "orders_2023_q1");
        TableId parentOrder = router.route(childOrder);
        assertThat(parentOrder.schema()).isEqualTo("public");
        assertThat(parentOrder.table()).isEqualTo("orders");

        // Test another child partition
        TableId childProduct = new TableId(null, "public", "products_electronics");
        TableId parentProduct = router.route(childProduct);
        assertThat(parentProduct.schema()).isEqualTo("public");
        assertThat(parentProduct.table()).isEqualTo("products_by_category");
    }

    @Test
    void testBasicPartitionRouting_WithLegacyIndexFormat() {
        // Test the legacy format where partition patterns map to parent tables by index
        String tables = "public.orders,public.products_by_category";
        String partitionTables = "public\\.orders_.*,public\\.products_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Child partitions should route to corresponding parent by index
        TableId childOrder = new TableId(null, "public", "orders_2023_q1");
        TableId parentOrder = router.route(childOrder);
        assertThat(parentOrder.table()).isEqualTo("orders");

        TableId childProduct = new TableId(null, "public", "products_electronics");
        TableId parentProduct = router.route(childProduct);
        assertThat(parentProduct.table()).isEqualTo("products_by_category");
    }

    @Test
    void testNonPartitionTablePassThrough() {
        // Non-partition tables should pass through unchanged
        String tables = "public.orders,public.customers";
        String partitionTables = "public.orders:public\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Regular table should not be routed
        TableId customers = new TableId(null, "public", "customers");
        TableId routed = router.route(customers);
        assertThat(routed).isEqualTo(customers);
        assertThat(router.isChildTable(customers)).isFalse();
    }

    @Test
    void testRoutingDisabled() {
        // When includePartitionedTables is false, no routing should occur
        String tables = "public.orders";
        String partitionTables = "public.orders:public\\.orders_.*";

        PostgresPartitionRouter router =
                new PostgresPartitionRouter(false, tables, partitionTables);

        TableId childOrder = new TableId(null, "public", "orders_2023_q1");
        TableId routed = router.route(childOrder);
        // Should return the original table ID when routing is disabled
        assertThat(routed).isEqualTo(childOrder);
    }

    // =====================================================================================
    // Test: Representative Tables Routing (Batch Processing)
    // =====================================================================================

    @Test
    void testRouteRepresentativeTables() {
        String tables = "partition.orders,partition.products_by_category";
        String partitionTables =
                "partition.orders:partition\\.orders_.*,partition.products_by_category:partition\\.products_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Input: Mix of parent, child, and regular tables
        List<TableId> capturedTables =
                Arrays.asList(
                        new TableId(null, "partition", "orders_2023_q1"),
                        new TableId(null, "partition", "orders_2023_q2"),
                        new TableId(null, "partition", "products_electronics"),
                        new TableId(null, "partition", "products_clothing"),
                        new TableId(null, "partition", "customers") // non-partition table
                        );

        Iterable<TableId> routed = router.routeRepresentativeTables(capturedTables);

        // Expected: Parent tables + non-partition tables (deduplicated)
        Set<TableId> routedSet = new LinkedHashSet<>();
        routed.forEach(routedSet::add);

        assertThat(routedSet).hasSize(3);
        assertThat(routedSet)
                .containsExactlyInAnyOrder(
                        new TableId(null, "partition", "orders"),
                        new TableId(null, "partition", "products_by_category"),
                        new TableId(null, "partition", "customers"));
    }

    @Test
    void testRouteRepresentativeTables_OrderPreserved() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        List<TableId> capturedTables =
                Arrays.asList(
                        new TableId(null, "public", "orders_2023_q1"),
                        new TableId(null, "public", "orders_2023_q2"),
                        new TableId(null, "public", "orders_2023_q3"));

        Iterable<TableId> routed = router.routeRepresentativeTables(capturedTables);

        // Result should be deduplicated but first occurrence order preserved
        Set<TableId> routedSet = new LinkedHashSet<>();
        routed.forEach(routedSet::add);

        assertThat(routedSet).hasSize(1);
        assertThat(routedSet.iterator().next().table()).isEqualTo("orders");
    }

    // =====================================================================================
    // Test: Database-Derived Partition Mapping (PG10 pg_inherits Support)
    // =====================================================================================

    @Test
    void testPreloadPartitionMappingFromDatabase() {
        // Start with pattern-based routing
        String tables = "public.orders";
        String partitionTables = "public.orders:public\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Simulate database-derived mappings from pg_inherits query
        Map<TableId, TableId> dbMappings = new HashMap<>();
        dbMappings.put(
                new TableId(null, "public", "orders_2023_q1"),
                new TableId(null, "public", "orders"));
        dbMappings.put(
                new TableId(null, "public", "orders_2023_q2"),
                new TableId(null, "public", "orders"));
        dbMappings.put(
                new TableId(null, "public", "orders_2023_q3"),
                new TableId(null, "public", "orders"));

        // Preload database mappings
        router.preloadPartitionMappingFromDatabase(dbMappings);

        // Database mappings should take precedence over pattern matching
        TableId q1 = new TableId(null, "public", "orders_2023_q1");
        assertThat(router.isChildTable(q1)).isTrue();
        assertThat(router.getPartitionParent(q1))
                .isEqualTo(Optional.of(new TableId(null, "public", "orders")));

        // Verify routing works correctly
        assertThat(router.route(q1).table()).isEqualTo("orders");
    }

    // =====================================================================================
    // Test: Parent Table Detection
    // =====================================================================================

    @Test
    void testIsConfiguredParent() {
        String tables = "partition.orders,partition.products_by_category";
        String partitionTables =
                "partition.orders:partition\\.orders_.*,partition.products_by_category:partition\\.products_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Parent tables should be recognized
        TableId orders = new TableId(null, "partition", "orders");
        assertThat(router.isConfiguredParent(orders)).isTrue();

        TableId products = new TableId(null, "partition", "products_by_category");
        assertThat(router.isConfiguredParent(products)).isTrue();

        // Child and non-partition tables should not be parents
        TableId childOrder = new TableId(null, "partition", "orders_2023_q1");
        assertThat(router.isConfiguredParent(childOrder)).isFalse();

        TableId customers = new TableId(null, "partition", "customers");
        assertThat(router.isConfiguredParent(customers)).isFalse();
    }

    @Test
    void testIsIncluded_WithPartitionRouting() {
        String tables = "partition.orders";
        String partitionTables = "partition.orders:partition\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Captured tables includes child partitions
        List<String> capturedTables =
                Arrays.asList("partition.orders_2023_q1", "partition.orders_2023_q2");

        // Parent table should be included because children route to it
        TableId parent = new TableId(null, "partition", "orders");
        assertThat(router.isIncluded(capturedTables, parent)).isTrue();

        // Non-partition table should not be included
        TableId customers = new TableId(null, "partition", "customers");
        assertThat(router.isIncluded(capturedTables, customers)).isFalse();
    }

    // =====================================================================================
    // Test: Pattern Normalization (Catalog Handling)
    // =====================================================================================

    @Test
    void testNormalizePatternIgnoreCatalog() {
        // Three-segment pattern: catalog.schema.table -> schema.table
        String pattern1 = "mydb.public.orders_.*";
        String normalized1 = PostgresPartitionRouter.normalizePatternIgnoreCatalog(pattern1);
        assertThat(normalized1).isEqualTo("public.orders_.*");

        // Escaped dots: catalog\.schema\.table -> schema\.table
        String pattern2 = "mydb\\.public\\.orders_.*";
        String normalized2 = PostgresPartitionRouter.normalizePatternIgnoreCatalog(pattern2);
        assertThat(normalized2).isEqualTo("public\\.orders_.*");

        // Two-segment pattern should remain unchanged
        String pattern3 = "public.orders_.*";
        String normalized3 = PostgresPartitionRouter.normalizePatternIgnoreCatalog(pattern3);
        assertThat(normalized3).isEqualTo("public.orders_.*");

        // No dots should remain unchanged
        String pattern4 = "orders";
        String normalized4 = PostgresPartitionRouter.normalizePatternIgnoreCatalog(pattern4);
        assertThat(normalized4).isEqualTo("orders");
    }

    // =====================================================================================
    // Test: Cache Behavior and Performance
    // =====================================================================================

    @Test
    void testCachingBehavior() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        TableId child = new TableId(null, "public", "orders_2023_q1");

        // First call should populate cache
        Optional<TableId> parent1 = router.getPartitionParent(child);
        assertThat(parent1).isPresent();
        assertThat(parent1.get().table()).isEqualTo("orders");

        // Second call should use cached result (same object)
        Optional<TableId> parent2 = router.getPartitionParent(child);
        assertThat(parent2).isEqualTo(parent1);
    }

    // =====================================================================================
    // Test: Edge Cases
    // =====================================================================================

    @Test
    void testEmptyPartitionTables() {
        String tables = "public.orders";
        String partitionTables = null;

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Without partition tables configured, no routing should occur
        TableId anyTable = new TableId(null, "public", "orders_2023_q1");
        assertThat(router.route(anyTable)).isEqualTo(anyTable);
        assertThat(router.isChildTable(anyTable)).isFalse();
    }

    @Test
    void testMultipleSchemas() {
        // Test routing across different schemas
        String tables = "schema1.orders,schema2.orders";
        String partitionTables =
                "schema1.orders:schema1\\.orders_.*,schema2.orders:schema2\\.orders_.*";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        TableId child1 = new TableId(null, "schema1", "orders_2023_q1");
        assertThat(router.route(child1).schema()).isEqualTo("schema1");
        assertThat(router.route(child1).table()).isEqualTo("orders");

        TableId child2 = new TableId(null, "schema2", "orders_2023_q1");
        assertThat(router.route(child2).schema()).isEqualTo("schema2");
        assertThat(router.route(child2).table()).isEqualTo("orders");
    }

    @Test
    void testComplexRegexPatterns() {
        // Test various regex patterns commonly used for partition tables
        String tables = "public.events";
        String partitionTables = "public.events:public\\.events_\\d{4}_\\d{2}"; // YYYY_MM format

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        TableId child1 = new TableId(null, "public", "events_2023_01");
        assertThat(router.isChildTable(child1)).isTrue();
        assertThat(router.route(child1).table()).isEqualTo("events");

        TableId child2 = new TableId(null, "public", "events_2023_12");
        assertThat(router.isChildTable(child2)).isTrue();

        // Non-matching pattern should not be routed
        TableId nonMatch = new TableId(null, "public", "events_q1");
        assertThat(router.isChildTable(nonMatch)).isFalse();
        assertThat(router.route(nonMatch)).isEqualTo(nonMatch);
    }

    // =====================================================================================
    // Test: Parent/Child With Table-Only Names (no schema)
    // =====================================================================================

    @Test
    void testRoutingWithTableOnlyParentAndChildPattern() {
        // Parent only provides table name; child pattern only provides table name
        String tables = "orders";
        String partitionTables = "orders:orders_\\d{6}";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // Child captured under a specific schema should inherit schema to parent
        TableId child = new TableId(null, "public", "orders_202401");
        TableId routed = router.route(child);

        assertThat(router.isChildTable(child)).isTrue();
        assertThat(routed.schema()).isEqualTo("public");
        assertThat(routed.table()).isEqualTo("orders");
    }

    // =====================================================================================
    // Test: Mixed Formats (catalog.schema.table, schema.table, and table-only)
    // =====================================================================================

    @Test
    void testRoutingWithMixedPatternFormats() {
        String tables = String.join(",", "aia_test.public.a", "public.b", "c");

        String partitionTables =
                String.join(
                        ",",
                        // three-segment (catalog.schema.table)
                        "aia_test.public.a:aia_test.public.a_\\d{6}",
                        // two-segment (schema.table)
                        "public.b:public.b_\\d{6}",
                        // table-only
                        "c:c_\\d{6}");

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        // a: catalog present in config; child in schema "public"
        TableId aChild = new TableId(null, "public", "a_202401");
        TableId aParent = router.route(aChild);
        assertThat(aParent.schema()).isEqualTo("public");
        assertThat(aParent.table()).isEqualTo("a");

        // b: two-segment in config
        TableId bChild = new TableId(null, "public", "b_202402");
        TableId bParent = router.route(bChild);
        assertThat(bParent.schema()).isEqualTo("public");
        assertThat(bParent.table()).isEqualTo("b");

        // c: table-only in config
        TableId cChild = new TableId(null, "public", "c_202403");
        TableId cParent = router.route(cChild);
        assertThat(cParent.schema()).isEqualTo("public");
        assertThat(cParent.table()).isEqualTo("c");
    }

    // =====================================================================================
    // Test: getSelectors should accept escaped three-segment patterns and match children
    // =====================================================================================

    @Test
    void testGetSelectorsWithEscapedThreeSegmentPatterns() {
        String tables = "aia_test.public.aia_t_icc_jjdb";
        String partitionTables =
                "aia_test.public.aia_t_icc_jjdb:aia_test\\.public\\.aia_t_icc_jjdb_\\d{6}";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);

        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        // Child with namespace/schema/table
        org.apache.flink.cdc.common.event.TableId child =
                org.apache.flink.cdc.common.event.TableId.tableId(
                        "aia_test", "public", "aia_t_icc_jjdb_202401");

        // Must match via child pattern after normalization and un-escaping for selectors
        assertThat(selectors.isMatch(child)).isTrue();
    }

    // =====================================================================================
    // Test: getReg/selectors composition (no-colon patterns)
    // Expect: child regex patterns + leftover parent tables only
    // =====================================================================================

    @Test
    void testGetSelectors_NoColon_PrioritizeChildPatternsAndKeepOnlyLeftoverParents() {
        String tables =
                String.join(
                        ",",
                        "aia_test.public.aia_t_icc_jjdb",
                        "aia_test.public.aia_t_icc_jjdb_extend",
                        "aia_test.public.aia_t_vcs_fkdb",
                        "aia_test.public.aia_t_dsrb");

        String partitionTables =
                String.join(
                        ",",
                        "aia_test.public.aia_t_icc_jjdb_\\d{6}",
                        "aia_test.public.aia_t_icc_jjdb_extend_\\d{6}",
                        "aia_test.public.aia_t_vcs_fkdb_\\d{6}");

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);
        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        // Matches (child partitions and leftover parent dsrb)
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_202401")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_extend_202402")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_vcs_fkdb_202403")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_dsrb")))
                .isTrue();

        // Non-matches: base parents that have child patterns, and unrelated tables
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_extend")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_vcs_fkdb")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "other")))
                .isFalse();
    }

    // =====================================================================================
    // Test: getReg/selectors composition (colon patterns)
    // Expect: child regex patterns + leftover parent tables only
    // =====================================================================================

    @Test
    void testGetSelectors_Colon_PrioritizeChildPatternsAndKeepOnlyLeftoverParents() {
        String tables =
                String.join(
                        ",",
                        "aia_test.public.aia_t_icc_jjdb",
                        "aia_test.public.aia_t_icc_jjdb_extend",
                        "aia_test.public.aia_t_vcs_fkdb",
                        "aia_test.public.aia_t_dsrb");

        String partitionTables =
                String.join(
                        ",",
                        "aia_test.public.aia_t_icc_jjdb:aia_test.public.aia_t_icc_jjdb_\\d{6}",
                        "aia_test.public.aia_t_icc_jjdb_extend:aia_test.public.aia_t_icc_jjdb_extend_\\d{6}",
                        "aia_test.public.aia_t_vcs_fkdb:aia_test.public.aia_t_vcs_fkdb_\\d{6}");

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);
        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        // Matches
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_202401")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_extend_202402")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_vcs_fkdb_202403")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_dsrb")))
                .isTrue();

        // Non-matches
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_icc_jjdb_extend")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "aia_t_vcs_fkdb")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "aia_test", "public", "other")))
                .isFalse();
    }

    // =====================================================================================
    // Test: Canonical exclude (ignore catalog) — no-colon, mixed segments
    // tables uses 2-seg, partition.tables uses 3-seg; parent should be excluded
    // =====================================================================================

    @Test
    void testGetSelectors_Canonical_NoColon_MixedSegments() {
        String tables = String.join(",", "public.orders", "public.dsrb");

        // child regex carries catalog+schema+table
        String partitionTables = "aia_test.public.orders_\\d{6}";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);
        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        // child should match (use 2-segment TableId as in runtime discovery)
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "orders_202401")))
                .isTrue();

        // leftover parent (no child regex): dsrb
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "dsrb")))
                .isTrue();

        // parent orders should be excluded (covered by child regex)
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "orders")))
                .isFalse();
    }

    // =====================================================================================
    // Test: Canonical exclude (ignore catalog) — colon, mixed segments
    // =====================================================================================

    @Test
    void testGetSelectors_Canonical_Colon_MixedSegments() {
        String tables = String.join(",", "public.orders", "public.dsrb");

        // colon with 3-seg parent:child; tables only 2-seg
        String partitionTables = "aia_test.public.orders:aia_test.public.orders_\\d{6}";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);
        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "orders_202401")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "dsrb")))
                .isTrue();

        // base parent should be excluded
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "orders")))
                .isFalse();
    }

    // =====================================================================================
    // Test: Table-only child regex excludes parents across schemas
    // =====================================================================================

    @Test
    void testGetSelectors_TableOnlyChildRegex_ExcludesAllSchemaParents() {
        String tables = String.join(",", "schema1.orders", "schema2.orders", "public.dsrb");

        // table-only child regex
        String partitionTables = "orders_\\d{6}";

        PostgresPartitionRouter router = new PostgresPartitionRouter(true, tables, partitionTables);
        org.apache.flink.cdc.common.schema.Selectors selectors = router.getSelectors();

        // Child should match in any schema
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "schema1", "orders_202401")))
                .isTrue();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "schema2", "orders_202402")))
                .isTrue();

        // Parents with table name 'orders' should be excluded even with different schemas
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "schema1", "orders")))
                .isFalse();
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "schema2", "orders")))
                .isFalse();

        // Leftover parent should remain
        assertThat(
                        selectors.isMatch(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        "public", "dsrb")))
                .isTrue();
    }
}
