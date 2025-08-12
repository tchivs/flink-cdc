package io.debezium.connector.postgresql;

import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresVersionUtils;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PostgresPartitionSchema extends PostgresSchema {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionSchema.class);

    private final PostgresConnection jdbcConnection;
    private final ConcurrentMap<String, TableId> parentTableCache = new ConcurrentHashMap<>();

    // Cache for pre-computed table type information to avoid database queries
    private final ConcurrentMap<
                    TableId,
                    org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTableType>
            tableTypeCache = new ConcurrentHashMap<>();

    protected PostgresPartitionSchema(
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            PostgresDefaultValueConverter defaultValueConverter,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter,
            PostgresConnection jdbcConnection) {
        super(config, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    protected boolean isFilteredOut(TableId id) {
        boolean filteredOut = super.isFilteredOut(id);
        if (filteredOut) {
            // First check pre-computed table type cache to avoid database queries
            org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTableType tableType =
                    tableTypeCache.get(id);

            if (tableType != null) {
                // Use cached table type information - much faster than database query
                filteredOut = !tableType.isPartitioned();
                LOG.trace("Using cached table type {} for table {}", tableType, id);
            } else {
                // Fallback to database query only if no cached information is available
                LOG.debug("No cached table type for {}, querying database", id);
                try {
                    filteredOut = !PostgresVersionUtils.isPartitionTable(jdbcConnection, id);

                    // Cache the result for future use
                    if (!filteredOut) {
                        tableTypeCache.put(
                                id,
                                org.apache.flink.cdc.connectors.postgres.source.utils
                                        .PostgresTableType.PARTITIONED_TABLE);
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to check partition status for table {}, treating as filtered",
                            id,
                            e);
                    filteredOut = true;
                }
            }
        }
        return filteredOut;
    }

    @Override
    public Table tableFor(TableId id) {
        Table table = super.tableFor(id);
        if (table == null) {
            table = tableForPartition(id);
        }
        return table;
    }

    @Nullable
    private Table tableForPartition(TableId id) {
        Tables tables = this.tables();
        // Check cache first
        String tableKey = id.identifier();
        TableId parentTableId = parentTableCache.get(tableKey);
        if (parentTableId != null) {
            return tables.forTable(parentTableId);
        }

        // Find parent table using PostgresVersionUtils
        try {
            Optional<TableId> resolvedParentId =
                    PostgresVersionUtils.findParentTableForPartition(jdbcConnection, id);
            if (resolvedParentId.isPresent()) {
                // Cache the result for future lookups
                parentTableCache.put(tableKey, resolvedParentId.get());
                return tables.forTable(resolvedParentId.get());
            }
        } catch (Exception e) {
            LOG.warn("Failed to find parent table for partition {}", id, e);
        }
        return null;
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        super.buildAndRegisterSchema(table);
    }

    /** Clears the partition table cache. Useful for testing or when table structure changes. */
    public void clearCache() {
        parentTableCache.clear();
        tableTypeCache.clear();
    }

    /** Returns the number of cached parent table mappings. */
    public int getCacheSize() {
        return parentTableCache.size();
    }

    /**
     * Sets the table type cache with pre-computed table type information. This method allows bulk
     * setting of table types to avoid individual database queries during schema operations.
     *
     * @param tableTypes a map of TableId to PostgresTableType
     */
    public void setTableTypeCache(
            java.util.Map<
                            TableId,
                            org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTableType>
                    tableTypes) {
        this.tableTypeCache.putAll(tableTypes);
        LOG.debug("Set table type cache for {} tables", tableTypes.size());
    }

    /**
     * Returns the number of cached table type mappings.
     *
     * @return the size of the table type cache
     */
    public int getTableTypeCacheSize() {
        return tableTypeCache.size();
    }

    /**
     * Checks if table type information is cached for the given table.
     *
     * @param tableId the table identifier
     * @return true if table type is cached
     */
    public boolean hasTableTypeInCache(TableId tableId) {
        return tableTypeCache.containsKey(tableId);
    }
}
