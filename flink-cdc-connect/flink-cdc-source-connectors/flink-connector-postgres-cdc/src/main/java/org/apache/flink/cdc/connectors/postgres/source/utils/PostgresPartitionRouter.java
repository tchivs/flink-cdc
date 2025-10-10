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

import org.apache.flink.cdc.common.schema.Selectors;

import io.debezium.relational.TableId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * postgresPartitionRouter handles the selection and routing of PostgreSQL partitioned tables.
 *
 * <p>This class maintains the mapping between child partition tables and their parent tables, and
 * provides methods to route events from partitions to their parent tables. This is particularly
 * useful for PostgreSQL 10+ where partitioned tables need special handling.
 *
 * <p><b>Partition Mapping Strategy:</b>
 *
 * <ul>
 *   <li><b>Pattern-based mapping:</b> Initially configured via {@code partition.tables} patterns
 *   <li><b>Database-derived mapping:</b> Can be populated with accurate pg_inherits data via {@link
 *       #preloadPartitionMappingFromDatabase(Map)}
 *   <li><b>Hybrid approach:</b> Database mappings take precedence over pattern matching when both
 *       exist
 * </ul>
 *
 * <p><b>Integration with PostgresPartitionRoutingSchema:</b> When used together with {@link
 * PostgresPartitionRoutingSchema}, the router benefits from database-queried partition
 * relationships injected via {@code preloadPartitionMappingFromDatabase()}. This provides more
 * accurate routing than pattern matching alone.
 */
public class PostgresPartitionRouter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Whether to include partitioned tables in the selection. */
    private final boolean includePartitionedTables;

    /** Comma-separated list of parent table patterns. */
    private final String tables;

    /** Comma-separated list of partition table patterns. */
    private final String partitionTables;

    /** Mapping from partition patterns to their parent TableId. */
    private transient volatile Map<Pattern, TableId> childToParentMap;

    /** Cache for resolved parent tables to avoid repeated pattern matching. */
    private transient volatile Map<TableId, Optional<TableId>> parentCache;

    /** Cached selectors for table matching. */
    private transient Selectors selectors;

    // No per-instance captured tables/ids are stored; routing is stateless

    // Parents configured via partitionTables (and fallback to tables), computed at construction
    private java.util.Set<String> configuredParentTables;

    // Cache derived from capturedTables -> routed parent includes
    private transient volatile java.util.Set<TableId> cachedIncludedParents;
    private transient volatile int cachedIncludedParentsHash;

    /**
     * Creates a new postgresPartitionRouter.
     *
     * @param includePartitionedTables whether to include partitioned tables
     * @param tables comma-separated list of parent table patterns
     * @param partitionTables comma-separated list of partition table patterns
     */
    public PostgresPartitionRouter(
            boolean includePartitionedTables, String tables, String partitionTables) {
        this.includePartitionedTables = includePartitionedTables;
        this.tables = tables;
        this.partitionTables = partitionTables;
        // Initialize the mapping eagerly in constructor
        initializePartitionMapping();
        // Pre-compute configured parent tables for fast inclusion check
        buildConfiguredParents();
    }
    // Overload with List<String> was removed; callers should pass pattern string or null.
    // Note: historical constructor with List<String> tables removed as unused

    /**
     * Initializes the mapping between child partition patterns and parent tables.
     *
     * <p>Supports two formats: 1. "parent:childPattern" format: partition tables has explicit
     * parent:child mappings 2. Legacy index-based format: partition patterns mapped to tables by
     * index position
     *
     * <p>This method is thread-safe and uses double-checked locking to ensure the map is only
     * initialized once.
     */
    private void initializePartitionMapping() {
        if (childToParentMap == null) {
            synchronized (this) {
                if (childToParentMap == null) {
                    Map<Pattern, TableId> tempMap = new ConcurrentHashMap<>();

                    if (this.partitionTables != null && !this.partitionTables.trim().isEmpty()) {
                        String[] partitionPatterns = this.partitionTables.split(",");

                        // Check if using "parent:child" format by examining first pattern
                        boolean isColonFormat =
                                partitionPatterns.length > 0 && partitionPatterns[0].contains(":");

                        if (isColonFormat) {
                            // Format: "parent:childPattern,parent2:childPattern2"
                            for (String patternStr : partitionPatterns) {
                                String[] parts = patternStr.split(":", 2);
                                if (parts.length < 2) {
                                    continue; // Skip invalid patterns
                                }
                                String parentStr = parts[0].trim();
                                String childPattern = parts[1].trim();

                                TableId parent = parseSchemaTableId(parentStr);
                                String normalizedPattern =
                                        normalizePatternIgnoreCatalog(childPattern);
                                Pattern combined =
                                        compileSchemaAndTableRegexPattern(normalizedPattern);
                                tempMap.put(combined, parent);
                            }
                        } else {
                            // Legacy format: map partition patterns to parent tables by index
                            String[] parentTables =
                                    tables != null ? tables.split(",") : new String[0];
                            for (int i = 0; i < partitionPatterns.length; i++) {
                                String patternStr = partitionPatterns[i].trim();
                                if (patternStr.isEmpty()) {
                                    continue;
                                }

                                String normalizedPattern =
                                        normalizePatternIgnoreCatalog(patternStr);
                                TableId parent = null;

                                // Map partition pattern to corresponding parent table by index
                                if (i < parentTables.length) {
                                    String parentStr = parentTables[i].trim();
                                    if (!parentStr.isEmpty()) {
                                        parent = parseSchemaTableId(parentStr);
                                    }
                                }

                                Pattern combined =
                                        compileSchemaAndTableRegexPattern(normalizedPattern);
                                tempMap.put(combined, parent);
                            }
                        }
                    }

                    childToParentMap = tempMap;
                }
            }
        }
    }

    /**
     * Compile a regex pattern where only the table name is treated as regex and schema (if
     * provided) is treated as a literal. The input should be normalized to ignore catalog if it was
     * present.
     *
     * <p>Examples: - "public.orders_\\d+" -> Pattern for "public\\." + orders_\\d+ - "orders_\\d+"
     * -> Pattern for orders_\\d+ (table-only)
     */
    private static Pattern compileSchemaAndTableRegexPattern(String normalized) {
        if (normalized == null || normalized.isEmpty()) {
            return Pattern.compile(normalized == null ? "" : normalized);
        }

        // Try to split by escaped dot first (schema literal expressed as "schema\.tableRegex")
        int escDot = normalized.indexOf("\\.");
        if (escDot >= 0) {
            String schema = normalized.substring(0, escDot);
            String tableRegex = normalized.substring(escDot + 2);
            String finalRegex = Pattern.quote(schema) + "\\." + tableRegex;
            return Pattern.compile(finalRegex);
        }

        // Then try to split by plain dot ("schema.tableRegex")
        int dot = normalized.indexOf('.');
        if (dot >= 0) {
            String schema = normalized.substring(0, dot);
            String tableRegex = normalized.substring(dot + 1);
            String finalRegex = Pattern.quote(schema) + "\\." + tableRegex;
            return Pattern.compile(finalRegex);
        }

        // No schema provided: treat as table-only regex
        return Pattern.compile(normalized);
    }

    /**
     * Gets the selectors for table matching.
     *
     * @return the Selectors instance
     */
    public Selectors getSelectors() {
        if (selectors == null) {
            synchronized (this) {
                if (selectors == null) {
                    if (includePartitionedTables && partitionTables != null) {
                        selectors =
                                new Selectors.SelectorsBuilder()
                                        .includeTables(getReg(tables, this.partitionTables))
                                        .build();
                    } else {
                        selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
                    }
                }
            }
        }
        return selectors;
    }

    private String getReg(String tables, String partitionTables) {
        if ((tables == null || tables.trim().isEmpty())
                && (partitionTables == null || partitionTables.trim().isEmpty())) {
            return null;
        }

        java.util.LinkedHashSet<String> childPatterns = new java.util.LinkedHashSet<>();
        java.util.LinkedHashSet<String> parentTables = new java.util.LinkedHashSet<>();

        // Collect schema set from tables to expand table-only child regex patterns
        java.util.LinkedHashSet<String> schemaSet = new java.util.LinkedHashSet<>();
        if (tables != null && !tables.trim().isEmpty()) {
            for (String t : tables.split(",")) {
                String tt = t.trim();
                if (tt.isEmpty()) continue;
                TableId p = parseSchemaTableId(tt);
                if (p.schema() != null && !p.schema().isEmpty()) {
                    schemaSet.add(p.schema());
                }
            }
        }

        if (partitionTables != null && !partitionTables.trim().isEmpty()) {
            String[] entries = partitionTables.split(",");
            for (String entry : entries) {
                if (entry == null) continue;
                String e = entry.trim();
                if (e.isEmpty()) continue;
                int idx = e.indexOf(':');

                // Extract parent table from "parent:child" format
                if (idx > 0) {
                    String parentStr = e.substring(0, idx).trim();
                    if (!parentStr.isEmpty()) {
                        parentTables.add(parentStr);
                    }
                }

                String child = idx >= 0 ? e.substring(idx + 1).trim() : e;
                if (!child.isEmpty()) {
                    // For selectors, ensure dots between segments are unescaped so that
                    // namespace/schema/table are recognized as separate segments.
                    String childForSelectors = child.replace("\\.", ".");

                    // Expand table-only child regex to each schema from 'tables'
                    if (!childForSelectors.contains(".")) {
                        if (!schemaSet.isEmpty()) {
                            for (String schema : schemaSet) {
                                childPatterns.add(schema + "." + childForSelectors);
                            }
                        } else {
                            // Fallback: keep as-is when we don't know schemas
                            childPatterns.add(childForSelectors);
                        }
                    } else {
                        childPatterns.add(childForSelectors);
                    }
                }
            }
        }

        java.util.LinkedHashSet<String> result = new java.util.LinkedHashSet<>(childPatterns);

        // Add parent tables from partition.tables configuration
        result.addAll(parentTables);

        // Add all tables from 'tables' parameter
        if (tables != null && !tables.trim().isEmpty()) {
            for (String t : tables.split(",")) {
                String tt = t.trim();
                if (!tt.isEmpty()) {
                    result.add(tt);
                }
            }
        }

        if (result.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String s : result) {
            sb.append(s).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    // Historical capturedTables accessors removed as unused

    /**
     * Updates the captured table IDs and returns the routed table IDs.
     *
     * <p>This method routes child partition tables to their parent tables, returning only parent
     * tables and non-partitioned tables in a deduplicated and order-preserving manner.
     *
     * <p><b>Performance optimization:</b> This method uses a cache to avoid repeated pattern
     * matching for the same table IDs. For large numbers of partitions, this significantly reduces
     * the time complexity from O(n*m) to O(n) where n is the number of tables and m is the number
     * of patterns.
     *
     * @param capturedTableIds list of captured table IDs
     * @return iterable of routed table IDs (parents and non-partitioned tables)
     */
    public Iterable<TableId> routeRepresentativeTables(List<TableId> capturedTableIds) {
        initializePartitionMapping();
        initializeParentCache();

        // Use LinkedHashSet to maintain insertion order and deduplicate
        LinkedHashSet<TableId> routedTables = new LinkedHashSet<>();

        // Batch resolve all parents at once using cache
        for (TableId tableId : capturedTableIds) {
            Optional<TableId> parent = getPartitionParentCached(tableId);
            if (parent.isPresent()) {
                // This is a child table, add its parent
                routedTables.add(parent.get());
            } else {
                // This is a regular table (not a partition child), add it directly
                routedTables.add(tableId);
            }
        }

        return routedTables;
    }

    /**
     * Backwards-compatible alias of routeRepresentativeTables.
     *
     * @deprecated Use {@link #routeRepresentativeTables(List)} instead. This method will be removed
     *     in a future version.
     */
    @Deprecated
    public Iterable<TableId> updateTableIdMap(List<TableId> capturedTableIds) {
        return routeRepresentativeTables(capturedTableIds);
    }

    /**
     * Initializes the parent cache if not already initialized.
     *
     * <p>This cache is used to store the results of parent lookups to avoid repeated pattern
     * matching operations.
     */
    private void initializeParentCache() {
        if (parentCache == null) {
            synchronized (this) {
                if (parentCache == null) {
                    parentCache = new ConcurrentHashMap<>();
                }
            }
        }
    }

    private void buildConfiguredParents() {
        java.util.LinkedHashSet<String> set = new java.util.LinkedHashSet<>();
        if (partitionTables != null && !partitionTables.trim().isEmpty()) {
            String[] items = partitionTables.split(",");
            for (String item : items) {
                if (item == null || item.trim().isEmpty()) {
                    continue;
                }
                int idx = item.indexOf(':');
                if (idx > 0) {
                    String parentStr = item.substring(0, idx).trim();
                    if (!parentStr.isEmpty()) {
                        TableId parsed = parseSchemaTableId(parentStr);
                        set.add(parsed.identifier());
                    }
                }
            }
        } else if (tables != null && !tables.trim().isEmpty()) {
            String[] parents = tables.split(",");
            for (String parentStr : parents) {
                if (parentStr == null || parentStr.trim().isEmpty()) {
                    continue;
                }
                TableId parsed = parseSchemaTableId(parentStr.trim());
                set.add(parsed.identifier());
            }
        }
        configuredParentTables = set;
    }

    /**
     * Gets the parent table for a given partition table using cache.
     *
     * <p>This method checks the cache first. If the result is not cached, it performs the pattern
     * matching and caches the result for future use.
     *
     * @param tableId the partition table ID
     * @return Optional containing the parent TableId if this is a child partition, empty otherwise
     */
    private Optional<TableId> getPartitionParentCached(TableId tableId) {
        // Check cache first
        Optional<TableId> cached = parentCache.get(tableId);
        if (cached != null) {
            return cached;
        }

        // Not in cache, perform pattern matching
        initializePartitionMapping();
        List<String> candidates = buildNameCandidates(tableId);
        for (Map.Entry<Pattern, TableId> entry : childToParentMap.entrySet()) {
            Pattern pattern = entry.getKey();
            for (String candidate : candidates) {
                if (pattern.matcher(candidate).matches()) {
                    // Found a match - this is a child table with a parent
                    TableId mappedParent = entry.getValue();
                    // If parent schema not specified (only table name provided), inherit child's
                    // schema
                    if (mappedParent != null
                            && mappedParent.schema() == null
                            && tableId.schema() != null) {
                        mappedParent = new TableId(null, tableId.schema(), mappedParent.table());
                    }
                    Optional<TableId> parent = Optional.ofNullable(mappedParent);
                    parentCache.put(tableId, parent);
                    return parent;
                }
            }
        }

        // No match found - this is not a child partition table
        Optional<TableId> notChild = Optional.empty();
        parentCache.put(tableId, notChild);
        return notChild;
    }

    /**
     * Checks if the given table is a child partition table.
     *
     * <p>This method uses a cache for performance optimization.
     *
     * @param tableId the table ID to check
     * @return true if the table is a child partition, false otherwise
     */
    public boolean isChildTable(TableId tableId) {
        initializeParentCache();
        Optional<TableId> parent = getPartitionParentCached(tableId);
        return parent.isPresent();
    }

    /**
     * Gets the parent table for a given partition table.
     *
     * <p>This method uses a cache for performance optimization.
     *
     * @param tableId the partition table ID
     * @return Optional containing the parent TableId if found, empty otherwise
     */
    public Optional<TableId> getPartitionParent(TableId tableId) {
        initializeParentCache();
        return getPartitionParentCached(tableId);
    }

    /**
     * Route table to its parent if includePartitionedTables is enabled and a parent exists.
     * Otherwise return itself.
     */
    public TableId route(TableId tableId) {
        if (!includePartitionedTables) {
            return tableId;
        }
        return getPartitionParent(tableId).orElse(tableId);
    }

    /**
     * Whether the given tableId should be considered included when routing partitions via parent.
     *
     * <p>If partition routing is enabled, a parent table is considered included if any configured
     * captured table (child or parent) routes to this parent.
     *
     * @param includePartitionedTables whether routing is enabled
     * @param capturedTables configured captured tables (schema.table strings)
     * @param tableId table to test
     * @return true if included by routing, otherwise false
     */
    public boolean isIncludeDataCollection(
            boolean includePartitionedTables, List<String> capturedTables, TableId tableId) {
        if (!includePartitionedTables || capturedTables == null || capturedTables.isEmpty()) {
            return false;
        }
        // Quick check: current table is one of configured parent tables
        if (isConfiguredParent(tableId)) {
            return true;
        }
        // Build or refresh cache if needed (based on List.hashCode content hash)
        int listHash = capturedTables.hashCode();
        java.util.Set<TableId> parents = cachedIncludedParents;
        if (parents == null || cachedIncludedParentsHash != listHash) {
            synchronized (this) {
                if (cachedIncludedParents == null || cachedIncludedParentsHash != listHash) {
                    java.util.LinkedHashSet<TableId> set = new java.util.LinkedHashSet<>();
                    for (String s : capturedTables) {
                        if (s == null || s.trim().isEmpty()) {
                            continue;
                        }
                        // Parse as schema.table format (PostgreSQL convention)
                        TableId parsed = parseSchemaTableId(s);
                        set.add(route(parsed));
                    }
                    cachedIncludedParents = set;
                    cachedIncludedParentsHash = listHash;
                    parents = set;
                } else {
                    parents = cachedIncludedParents;
                }
            }
        }
        return parents.contains(tableId);
    }

    /**
     * Returns whether the given table should be included according to the captured table list with
     * partition routing enabled for this router.
     */
    public boolean isIncluded(List<String> capturedTables, TableId tableId) {
        return isIncludeDataCollection(this.includePartitionedTables, capturedTables, tableId);
    }

    /** Alias of {@link #isIncluded(List, TableId)} for readability. */
    public boolean isBaseTable(List<String> capturedTables, TableId tableId) {
        return isIncluded(capturedTables, tableId);
    }

    /** Fast check whether the given tableId is one of the configured parent tables. */
    public boolean isConfiguredParent(TableId tableId) {
        if (configuredParentTables == null || configuredParentTables.isEmpty()) {
            return false;
        }
        // normalize for comparison (ignore catalog)
        TableId normalized = new TableId(null, tableId.schema(), tableId.table());
        return configuredParentTables.contains(normalized.identifier());
    }

    /**
     * Preloads partition mapping from pg_inherits system table.
     *
     * <p>This method populates the router's cache with actual database-derived partition
     * relationships, which take precedence over pattern-based matching. This is typically called by
     * {@link PostgresPartitionRoutingSchema} after querying pg_inherits.
     *
     * <p><b>Benefits of database-derived mapping:</b>
     *
     * <ul>
     *   <li>Accurate: Based on actual pg_inherits relationships, not pattern matching
     *   <li>Performance: Avoids repeated pattern matching for known partitions
     *   <li>Consistency: Ensures Schema and Router use identical partition mappings
     * </ul>
     *
     * @param childToParentMap map of child TableId to parent TableId from database query
     */
    public void preloadPartitionMappingFromDatabase(
            java.util.Map<TableId, TableId> childToParentMap) {
        initializeParentCache();
        for (java.util.Map.Entry<TableId, TableId> entry : childToParentMap.entrySet()) {
            parentCache.put(entry.getKey(), Optional.of(entry.getValue()));
        }
    }

    /**
     * Builds the schema.table name from a TableId, ignoring catalog.
     *
     * @param tableId the table ID
     * @return schema.table string
     */
    private static String buildSchemaTableName(TableId tableId) {
        String schema = tableId.schema() != null ? tableId.schema() : "";
        String table = tableId.table() != null ? tableId.table() : "";
        return schema + "." + table;
    }

    /**
     * Builds a list of candidate names for matching against patterns.
     *
     * <p>Currently returns only the schema.table format, ignoring catalog.
     *
     * @param tableId the table ID
     * @return list of candidate names
     */
    private static List<String> buildNameCandidates(TableId tableId) {
        List<String> candidates = new ArrayList<>(2);
        // schema.table for two-segment matching
        candidates.add(buildSchemaTableName(tableId));
        // table for single-segment matching
        if (tableId.table() != null) {
            candidates.add(tableId.table());
        }
        return candidates;
    }

    /**
     * Parses a schema.table string into a TableId with null catalog.
     *
     * <p>This method handles PostgreSQL table identifiers which are typically in "schema.table"
     * format (two segments), not the Debezium default "catalog.schema.table" (three segments).
     *
     * @param schemaTableStr the schema.table string (e.g., "public.orders")
     * @return TableId with null catalog and parsed schema and table
     */
    private static TableId parseSchemaTableId(String schemaTableStr) {
        if (schemaTableStr == null) {
            return new TableId(null, null, null);
        }

        String trimmed = schemaTableStr.trim();
        if (trimmed.isEmpty()) {
            return new TableId(null, null, "");
        }

        // Split by '.', support 1-segment (table), 2-segment (schema.table), 3+-segment
        // (catalog.schema.table...). We drop any leading catalog segments.
        String[] parts = trimmed.split("\\.");
        if (parts.length >= 3) {
            // Use the last two segments as schema and table
            String schema = parts[parts.length - 2];
            String table = parts[parts.length - 1];
            return new TableId(null, schema, table);
        } else if (parts.length == 2) {
            return new TableId(null, parts[0], parts[1]);
        } else {
            // Only table name provided
            return new TableId(null, null, parts[0]);
        }
    }

    /**
     * Normalizes a regex pattern to ignore catalog prefix during matching.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>"catalog.schema.table_\\d+" -> "schema.table_\\d+"
     *   <li>"catalog\\.schema\\.table_\\d+" -> "schema.table_\\d+"
     *   <li>"schema.table_\\d+" -> "schema.table_\\d+" (unchanged)
     * </ul>
     *
     * @param pattern the original pattern
     * @return normalized pattern without catalog prefix
     */
    static String normalizePatternIgnoreCatalog(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return pattern;
        }

        String trimmedPattern = pattern.trim();

        // If pattern doesn't contain dots, return as-is
        if (!trimmedPattern.contains(".")) {
            return trimmedPattern;
        }

        // Try to strip leading catalog segment only when pattern clearly has three segments.
        // Handle two common forms:

        // First, check for escaped dots (backslash-escaped): catalog\.schema\.table
        // We need to look for "\." which appears in the string as "\\." in Java
        int firstEscapedDot = trimmedPattern.indexOf("\\.");
        if (firstEscapedDot >= 0) {
            int secondEscapedDot = trimmedPattern.indexOf("\\.", firstEscapedDot + 2);
            if (secondEscapedDot > firstEscapedDot) {
                // Found two "\." occurrences - this is a three-segment pattern
                // Remove the leading "catalog\." part
                return trimmedPattern.substring(firstEscapedDot + 2);
            }
            // Only one "\." found - this is a two-segment pattern, return as-is
            return trimmedPattern;
        }

        // If no escaped dots, check for unescaped dots: catalog.schema.table
        // Count dots that are NOT part of regex patterns (like .*)
        int dotCount = 0;
        int firstSegmentDot = -1;
        for (int i = 0; i < trimmedPattern.length(); i++) {
            if (trimmedPattern.charAt(i) == '.') {
                // Check if this dot is part of a regex pattern (followed by * or +)
                if (i + 1 < trimmedPattern.length()) {
                    char next = trimmedPattern.charAt(i + 1);
                    if (next == '*' || next == '+' || next == '?' || next == '{') {
                        // This is a regex quantifier, not a separator
                        continue;
                    }
                }
                dotCount++;
                if (firstSegmentDot < 0) {
                    firstSegmentDot = i;
                }
            }
        }

        // If we have 2 or more separator dots, strip the first segment (catalog)
        if (dotCount >= 2 && firstSegmentDot > 0) {
            return trimmedPattern.substring(firstSegmentDot + 1);
        }

        // Fallback: return original pattern (two-segment or other forms)
        return trimmedPattern;
    }
}
