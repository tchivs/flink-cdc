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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnectionUtils;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.data.Envelope;
import io.debezium.relational.*;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A PostgresSchema wrapper that routes partition table events to their parent tables.
 *
 * <p>For PostgreSQL 10+ partitioned tables, this class provides:
 *
 * <ul>
 *   <li>Child-to-parent routing: All partition child events are routed to parent table
 *   <li>Schema optimization: Loads one representative child per parent (to get primary keys)
 *   <li>On-demand loading: Lazily loads schemas for tables not in initial preload
 * </ul>
 *
 * <p>Note: In PostgreSQL 10, primary keys are defined on child partitions, not parent tables.
 * Therefore, we must load at least one child partition's schema to get the complete table
 * definition including primary keys.
 *
 * <p>This is particularly useful for PostgreSQL 10 where the 'publish_via_partition_root' parameter
 * is not available.
 */
public class PostgresPartitionRoutingSchema extends PostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionRoutingSchema.class);

    private final PostgresConnectorConfig dbzConfig;
    private final PostgresConnection jdbcConnection;
    private final boolean routeToParent;

    /** Mapping from child partition table to its parent table. */
    private final Map<TableId, TableId> childToParent = new ConcurrentHashMap<>();

    /** Mapping from parent table to its representative child (used to get primary keys). */
    private final Map<TableId, TableId> parentToRepresentative = new ConcurrentHashMap<>();

    private final TopicSelector<TableId> topicSelector;
    private final TableSchemaBuilder schemaBuilder;
    private final String schemaPrefix;
    private final ColumnMappers columnMappers;
    private final ConcurrentMap<TableId, TableSchema> values = new ConcurrentHashMap<>();
    private Tables cacheTables;

    public PostgresPartitionRoutingSchema(
            PostgresConnection jdbcConnection,
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter,
            boolean lazyInit)
            throws SQLException {
        super(
                config,
                typeRegistry,
                jdbcConnection.getDefaultValueConverter(),
                topicSelector,
                valueConverter);
        this.dbzConfig = config;
        this.jdbcConnection = jdbcConnection;
        this.routeToParent =
                config.getConfig()
                        .getBoolean(PostgresSourceConfig.ROUTE_PARTITION_EVENTS_TO_PARENT, false);
        this.topicSelector = topicSelector;
        schemaBuilder =
                getTableSchemaBuilder(
                        config, valueConverter, jdbcConnection.getDefaultValueConverter());
        this.schemaPrefix = getSchemaPrefix(config.getLogicalName());
        this.columnMappers = ColumnMappers.create(config);
        LOG.info(
                "PostgresPartitionRoutingSchema initialized. routeToParent={}, db={}",
                this.routeToParent,
                config.databaseName());
        if (routeToParent) {
            if (!lazyInit) {
                preloadParentTables();
            } else {
                LOG.info("Partition routing uses lazy initialization (single-table reads)");
            }
        } else {
            // Default behavior: initialize by full refresh with include filters
            super.refresh(jdbcConnection, false);
        }
    }

    // Backwards-compatible constructor (eager by default)
    public PostgresPartitionRoutingSchema(
            PostgresConnection jdbcConnection,
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter)
            throws SQLException {
        this(jdbcConnection, config, typeRegistry, topicSelector, valueConverter, false);
    }

    private static TableSchemaBuilder getTableSchemaBuilder(
            PostgresConnectorConfig config,
            PostgresValueConverter valueConverter,
            PostgresDefaultValueConverter defaultValueConverter) {
        return new TableSchemaBuilder(
                valueConverter,
                defaultValueConverter,
                config.schemaNameAdjustmentMode().createAdjuster(),
                config.customConverterRegistry(),
                config.getSourceInfoStructMaker().schema(),
                config.getSanitizeFieldNames(),
                false);
    }

    @Override
    public Set<TableId> tableIds() {
        if (!routeToParent) {
            return super.tableIds();
        }
        return this.parentToRepresentative.keySet();
    }

    @Override
    public Table tableFor(TableId id) {
        if (!routeToParent) {
            return super.tableFor(id);
        }
        Table table = super.tableFor(id);
        if (table != null) {
            // return parent table name for
            // org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher.shouldEmit
            return table.edit().tableId(resolveToParentOrSelf(id)).create();
        }
        // Route to parent table if this is a child partition
        TableId tableId = resolveToParentOrSelf(id);
        Table parentTable = super.tableFor(tableId);
        if (parentTable != null) {
            return parentTable;
        }
        // Table not in cache, try to load it
        LOG.debug("tableFor({}) → parent={} not in cache, attempting load", id, id);
        try {
            return loadSingleTable(id, tableId);
        } catch (SQLException e) {
            LOG.warn("Failed to load  tableFor({})", id, e);
        }

        LOG.debug("tableFor({}) → unable to load, returning null", id);
        return null;
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        if (!routeToParent) {
            return super.schemaFor(id);
        }
        TableId oid = resolveToParentOrSelf(id);
        TableSchema tableSchema = values.get(oid);
        if (tableSchema != null) {
            return tableSchema;
        }
        // First check if we already have schema for this exact table ID
        TableSchema existingSchema = super.schemaFor(oid);
        if (existingSchema != null) {
            return existingSchema;
        }

        Table parentTable = this.cacheTables.forTable(oid);

        return extracted(parentTable);
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }

    private TableSchema extracted(Table table) {
        TableSchema schema =
                schemaBuilder.create(
                        schemaPrefix,
                        getEnvelopeSchemaName(table),
                        table,
                        dbzConfig.getColumnFilter(),
                        columnMappers,
                        dbzConfig.getKeyMapper());
        values.put(table.id(), schema);
        return schema;
    }

    private String getEnvelopeSchemaName(Table table) {
        return Envelope.schemaName(topicSelector.topicNameFor(table.id()));
    }

    private static String getSchemaPrefix(String serverName) {
        if (serverName == null) {
            return "";
        } else {
            serverName = serverName.trim();
            return serverName.endsWith(".") || serverName.isEmpty() ? serverName : serverName + ".";
        }
    }

    /**
     * Preloads parent table schemas for all captured partition tables.
     *
     * <p>This method:
     *
     * <ol>
     *   <li>Discovers all captured tables based on include filters
     *   <li>Queries pg_inherits to build child-to-parent mapping
     *   <li>Selects one representative child per parent (to get primary keys)
     *   <li>Loads schemas from representative children and stores under parent table IDs
     * </ol>
     *
     * <p>Note: In PostgreSQL 10, primary keys are defined on child partitions, not parent tables.
     * Therefore, we load schemas from a representative child partition (lexicographically first) to
     * obtain the complete table definition including primary keys.
     */
    private void preloadParentTables() throws SQLException {
        long start = System.currentTimeMillis();
        Set<TableId> allTables;
        // Step 1: Discover all captured tables based on include filters
        if (PostgresConnectionUtils.shouldExcludePartitionTables(jdbcConnection)) {
            allTables = jdbcConnection.getAllNonPartitionedTableIds(dbzConfig.databaseName());
        } else {
            allTables = jdbcConnection.getAllTableIds(dbzConfig.databaseName());
        }
        Tables.TableFilter includeFilter = dbzConfig.getTableFilters().dataCollectionFilter();
        Set<TableId> capturedTables = new HashSet<>();
        for (TableId table : allTables) {
            if (includeFilter.isIncluded(table)) {
                capturedTables.add(table);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Discovered {} captured tables", capturedTables.size());
        }

        // Step 2: Build child-to-parent mapping from pg_inherits
        // Only query partitions for captured tables to improve performance
        String sql =
                "SELECT cn.nspname AS child_schema, cc.relname AS child_table, "
                        + "pn.nspname AS parent_schema, pc.relname AS parent_table "
                        + "FROM pg_inherits "
                        + "JOIN pg_class cc ON inhrelid = cc.oid "
                        + "JOIN pg_namespace cn ON cc.relnamespace = cn.oid "
                        + "JOIN pg_class pc ON inhparent = pc.oid "
                        + "JOIN pg_namespace pn ON pc.relnamespace = pn.oid "
                        + "WHERE EXISTS ("
                        + "  SELECT 1 FROM unnest(?::text[]) AS t "
                        + "  WHERE t = cn.nspname || '.' || cc.relname"
                        + ")";

        // Build array of captured table names for filtering
        String[] capturedTableArray =
                capturedTables.stream()
                        .map(t -> t.schema() + "." + t.table())
                        .toArray(String[]::new);

        jdbcConnection.prepareQuery(
                sql,
                st -> {
                    java.sql.Array sqlArray =
                            st.getConnection().createArrayOf("text", capturedTableArray);
                    st.setArray(1, sqlArray);
                },
                rs -> {
                    while (rs.next()) {
                        TableId child =
                                new TableId(
                                        null,
                                        rs.getString("child_schema"),
                                        rs.getString("child_table"));
                        TableId parent =
                                new TableId(
                                        null,
                                        rs.getString("parent_schema"),
                                        rs.getString("parent_table"));
                        childToParent.put(child, parent);
                    }
                });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Built child-to-parent mapping for {} partitions", childToParent.size());
        }

        // Step 3: Select representative child for each parent table
        // Use lexicographically first child to get primary keys
        Map<TableId, List<TableId>> parentToChildren = new HashMap<>();
        for (TableId table : capturedTables) {
            TableId parent = childToParent.get(table);
            if (parent != null) {
                parentToChildren.computeIfAbsent(parent, k -> new ArrayList<>()).add(table);
            }
        }

        for (Map.Entry<TableId, List<TableId>> entry : parentToChildren.entrySet()) {
            TableId parent = entry.getKey();
            List<TableId> children = entry.getValue();
            children.stream()
                    .min(Comparator.comparing(TableId::schema).thenComparing(TableId::table))
                    .ifPresent(
                            representative -> parentToRepresentative.put(parent, representative));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Selected {} representative children for parent tables",
                    parentToRepresentative.size());
        }

        // Step 4: Identify which tables to load
        // - Load representative children for parent tables (to get primary keys)
        // - Load regular non-partition tables
        Set<TableId> tablesToLoad = new HashSet<>();
        for (TableId table : capturedTables) {
            TableId parent = childToParent.get(table);
            if (parent != null) {
                // This is a child partition, load its representative sibling
                TableId representative = parentToRepresentative.get(parent);
                if (representative != null) {
                    tablesToLoad.add(representative);
                }
            } else {
                // Not a child partition, add itself
                tablesToLoad.add(table);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Will load {} representative/regular tables", tablesToLoad.size());
        }

        // Step 5: Load schemas for identified tables
        cacheTables = new Tables();
        jdbcConnection.readSchema(
                cacheTables, dbzConfig.databaseName(), null, tablesToLoad::contains, null, false);
        // Step 6: Store loaded schemas under parent table IDs
        for (TableId tableId : tablesToLoad) {
            Table table = cacheTables.forTable(tableId);
            if (table != null) {
                TableId parent = childToParent.getOrDefault(tableId, tableId);
                if (parent != null) {
                    Table parentTable = cloneTableWithId(table, parent);
                    buildAndRegisterSchema(parentTable);
                    cacheTables.overwriteTable(parentTable);
                } else {
                    buildAndRegisterSchema(table);
                }
            }
        }

        long duration = System.currentTimeMillis() - start;
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "PostgresPartitionRoutingSchema loaded {} tables in {} ms "
                            + "({} captured tables, {} partition mappings, {} parents with representatives)",
                    tables().size(),
                    duration,
                    capturedTables.size(),
                    childToParent.size(),
                    parentToRepresentative.size());
        }
    }

    /**
     * Lazily loads the child-to-parent mapping for a specific table.
     *
     * <p>This method queries pg_inherits to check if the given table is a partition child, and if
     * so, adds it to the childToParent mapping. If a new parent is discovered and it's not yet in
     * the representative mapping, it also updates parentToRepresentative.
     *
     * @param tableId the table to check
     */
    private void ensureMappingFor(TableId tableId) throws SQLException {
        if (childToParent.containsKey(tableId)) {
            return;
        }

        String sql =
                "SELECT pn.nspname AS parent_schema, pc.relname AS parent_table "
                        + "FROM pg_inherits "
                        + "JOIN pg_class cc ON inhrelid = cc.oid "
                        + "JOIN pg_namespace cn ON cc.relnamespace = cn.oid "
                        + "JOIN pg_class pc ON inhparent = pc.oid "
                        + "JOIN pg_namespace pn ON pc.relnamespace = pn.oid "
                        + "WHERE cn.nspname = ? AND cc.relname = ?";

        final TableId[] parentHolder = new TableId[1];
        jdbcConnection.prepareQuery(
                sql,
                st -> {
                    st.setString(1, tableId.schema());
                    st.setString(2, tableId.table());
                },
                rs -> {
                    if (rs.next()) {
                        parentHolder[0] =
                                new TableId(
                                        null,
                                        rs.getString("parent_schema"),
                                        rs.getString("parent_table"));
                    }
                });

        if (parentHolder[0] != null) {
            TableId parent = parentHolder[0];
            childToParent.put(tableId, parent);
            LOG.debug("Lazy-loaded partition mapping: {} → {}", tableId, parent);

            // If this is a new parent we haven't seen, update representative mapping
            // Use this child as representative if none exists yet
            if (!parentToRepresentative.containsKey(parent)) {
                parentToRepresentative.put(parent, tableId);
                LOG.debug("Set {} as representative child for parent {}", tableId, parent);
            }
        }
    }

    /**
     * Loads schema for a single table and caches it.
     *
     * <p>For partition parent tables, this method will find a representative child partition and
     * load its schema (to get primary keys), then store it under the parent table ID.
     *
     * @param tableId the table to load (could be parent or regular table)
     * @return
     */
    private Table loadSingleTable(TableId tableId, TableId parentId) throws SQLException {

        if (cacheTables != null) {
            // Clone child schema with parent table ID
            // buildAndRegisterSchema(parentTable);
            Table childTable = cacheTables.forTable(parentId);
            if (childTable != null) {
                return childTable.edit().tableId(tableId).create();
            }
        }
        Tables tables = new Tables();
        jdbcConnection.readSchema(
                tables,
                dbzConfig.databaseName(),
                null,
                t -> Objects.equals(t, tableId),
                null,
                false);
        Table table = tables.forTable(tableId);
        Table parentTable = cloneTableWithId(table, parentId);
        buildAndRegisterSchema(parentTable);
        return table;
    }

    /**
     * Gets the child-to-parent mapping discovered from the database.
     *
     * <p>This mapping is populated by querying pg_inherits system table and represents the actual
     * partition relationships in the database. It can be used to populate other components (like
     * PostgresPartitionRouter) with accurate database-derived partition information instead of
     * relying solely on pattern matching.
     *
     * @return unmodifiable map of child partition tables to their parent tables
     */
    public Map<TableId, TableId> getChildToParentMapping() {
        return Collections.unmodifiableMap(childToParent);
    }

    /**
     * Resolves a table ID to its parent table if it's a partition child, or returns itself if it's
     * not a partition.
     *
     * <p>This method is useful for routing partition table events to their parent tables. For
     * example, if the input is a child partition table, this method returns the parent table ID. If
     * the input is a regular table (not a partition), it returns the same table ID.
     *
     * <p>If routing to parent is disabled, this method always returns the input table ID unchanged.
     *
     * @param tableId the table ID to resolve (could be child partition or regular table)
     * @return the parent table ID if tableId is a partition child, otherwise returns tableId itself
     */
    public TableId resolveToParentOrSelf(TableId tableId) {
        if (!routeToParent) {
            return tableId;
        }
        return childToParent.getOrDefault(tableId, tableId);
    }

    /**
     * Clones a table structure with a new table ID.
     *
     * <p>This is used to create partition child table definitions from parent table schemas,
     * allowing Debezium to process child partition events using the parent's schema.
     *
     * @param source the source table to clone
     * @param newId the new table ID for the cloned table
     * @return a new Table instance with the new ID and same schema as source
     */
    private Table cloneTableWithId(Table source, TableId newId) {
        TableEditor editor = Table.editor().tableId(newId);
        if (source.defaultCharsetName() != null) {
            editor.setDefaultCharsetName(source.defaultCharsetName());
        }
        for (io.debezium.relational.Column col : source.columns()) {
            editor.addColumn(col);
        }

        editor.setPrimaryKeyNames(source.primaryKeyColumnNames());
        return editor.create();
    }
}
