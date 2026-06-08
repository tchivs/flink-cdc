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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PartitionAwareStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresScanFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionMapper;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnectionUtils;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;
import static io.debezium.connector.postgresql.Utils.currentOffset;

/** The dialect for Postgres. */
public class PostgresDialect implements JdbcDataSourceDialect {
    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "postgres-cdc-connector";

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDialect.class);

    private final PostgresSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    private transient CustomPostgresSchema schema;

    /**
     * Cached partition parent schemas resolved from a representative child partition. Backed by a
     * {@link ConcurrentHashMap} because the snapshot phase can issue {@link
     * #resolvePartitionParentSchema} from multiple threads (parallel chunk-snapshot readers). The
     * field is {@code transient} to avoid serializing derived cache state into the dialect
     * snapshot. It is lazily re-initialized after Java/Flink deserialization.
     */
    private transient volatile Map<TableId, TableChange> partitionParentSchemaCache =
            new ConcurrentHashMap<>();

    private transient volatile Set<TableId> nonPartitionChildCache = ConcurrentHashMap.newKeySet();

    @Nullable private PostgresStreamFetchTask streamFetchTask;

    /**
     * Live partition capture state, updated by {@link
     * org.apache.flink.cdc.connectors.postgres.source.fetch.PartitionAwareStreamFetchTask} during
     * streaming and by the snapshot-only stream split initialization path before bounded WAL
     * backfill. Runtime components read this via {@link #getChildToParentMapping()} to route child
     * partition table IDs to their parent table IDs.
     */
    private volatile PartitionCaptureState currentPartitionState = PartitionCaptureState.EMPTY;

    public PostgresDialect(PostgresSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    /** Returns the current child→parent mapping from the live partition state. */
    public Map<TableId, TableId> getChildToParentMapping() {
        return currentPartitionState.getChildToParent();
    }

    private Set<TableId> getNonPartitionChildCache() {
        Set<TableId> cache = nonPartitionChildCache;
        if (cache == null) {
            synchronized (this) {
                cache = nonPartitionChildCache;
                if (cache == null) {
                    cache = ConcurrentHashMap.newKeySet();
                    nonPartitionChildCache = cache;
                }
            }
        }
        return cache;
    }

    /**
     * Updates the live partition state. Called by {@link PartitionAwareStreamFetchTask} when new
     * partitions are discovered/reconciled, and by the snapshot-only bounded stream path to seed
     * the static child→parent routing map without enabling dynamic partition discovery.
     *
     * <p><b>Internal API</b>: This method is not intended for external use. The partition state
     * must only be updated by connector runtime code to maintain the single-source-of-truth
     * invariant.
     */
    @Internal
    public void setCurrentPartitionState(PartitionCaptureState state) {
        if (state == null) {
            // Null is unexpected from the streaming task: it means the caller lost the latest
            // routing state. We fall back to EMPTY (which disables routing) but log a warning so
            // operators can correlate with downstream "child tableId not in monitored splits"
            // surprises if any are observed.
            LOG.warn(
                    "PostgresDialect.setCurrentPartitionState was called with null; "
                            + "falling back to EMPTY state. Partition routing will be disabled "
                            + "until a non-null state is supplied.");
        }
        this.currentPartitionState = state != null ? state : PartitionCaptureState.EMPTY;
        getNonPartitionChildCache()
                .removeAll(this.currentPartitionState.getChildToParent().keySet());
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        PostgresConnectorConfig dbzConfig = postgresSourceConfig.getDbzConnectorConfig();

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbc =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        CONNECTION_NAME,
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));

        try {
            jdbc.connect();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    public PostgresConnection openJdbcConnection() {
        return (PostgresConnection) openJdbcConnection(sourceConfig);
    }

    public PostgresReplicationConnection openPostgresReplicationConnection(
            PostgresConnection jdbcConnection) {
        try {
            PostgresConnectorConfig pgConnectorConfig = sourceConfig.getDbzConnectorConfig();
            TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(pgConnectorConfig);
            PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                    newPostgresValueConverterBuilder(pgConnectorConfig);
            PostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            pgConnectorConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
            PostgresTaskContext taskContext =
                    PostgresObjectUtils.newTaskContext(pgConnectorConfig, schema, topicSelector);
            return (PostgresReplicationConnection)
                    createReplicationConnection(
                            taskContext, jdbcConnection, false, pgConnectorConfig);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresReplicationConnection", e);
        }
    }

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return currentOffset((PostgresConnection) jdbc);

        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public Offset displayCommittedOffset(JdbcSourceConfig sourceConfig) {

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return PostgresConnectionUtils.committedOffset(
                    (PostgresConnection) jdbc, getSlotName(), getPluginName());

        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // from Postgres docs:
        //
        // SQL is case insensitive about key words and identifiers,
        // except when identifiers are double-quoted to preserve the case
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new PostgresChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new PostgresChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            boolean includePartitionedTables =
                    ((PostgresSourceConfig) sourceConfig).includePartitionedTables();
            List<TableId> capturedTables =
                    TableDiscoveryUtils.listTables(
                            // there is always a single database provided
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            includePartitionedTables);
            if (!includePartitionedTables || capturedTables.isEmpty()) {
                return capturedTables;
            }

            PartitionCaptureState partitionState = discoverPartitionState(capturedTables, jdbc);
            setCurrentPartitionState(partitionState);

            if (partitionState == PartitionCaptureState.EMPTY
                    || partitionState.getParentToChildren().isEmpty()) {
                return capturedTables;
            }

            return expandPartitionParentsForSnapshot(capturedTables, partitionState);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    private List<TableId> expandPartitionParentsForSnapshot(
            List<TableId> capturedTables, PartitionCaptureState partitionState) {
        LinkedHashSet<TableId> snapshotTables = new LinkedHashSet<>();
        for (TableId capturedTable : capturedTables) {
            List<TableId> children =
                    partitionState
                            .getParentToChildren()
                            .getOrDefault(capturedTable, Collections.emptyList());
            if (children.isEmpty()) {
                snapshotTables.add(capturedTable);
            } else {
                snapshotTables.addAll(children);
            }
        }
        LOG.info(
                "Expanded {} captured table(s) to {} snapshot table(s) by partition children.",
                capturedTables.size(),
                snapshotTables.size());
        return new ArrayList<>(snapshotTables);
    }

    /**
     * Discovers partition routing state for the configured tables.
     *
     * <p>This should be called once during enumerator initialization, after {@link
     * #discoverDataCollections}. The returned state is then stored in the StreamSplit and passed to
     * the streaming runtime.
     *
     * @param parentTableIds parent tables or child partition tables discovered for snapshot
     *     splitting
     * @return partition capture state (EMPTY if routing is not needed)
     */
    public PartitionCaptureState discoverPartitionState(List<TableId> parentTableIds) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            PartitionCaptureState discovered = discoverPartitionState(parentTableIds, jdbc);
            setCurrentPartitionState(discovered);
            return discovered;
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Error discovering partition state: " + e.getMessage(), e);
        }
    }

    private PartitionCaptureState discoverPartitionState(
            List<TableId> parentTableIds, JdbcConnection jdbc) throws SQLException {
        PartitionCaptureState discovered =
                TableDiscoveryUtils.discoverPartitionState(
                        parentTableIds,
                        jdbc,
                        sourceConfig.includePartitionedTables(),
                        partitionPublicationName());
        if (!sourceConfig.includePartitionedTables()
                || parentTableIds == null
                || parentTableIds.isEmpty()) {
            return discovered;
        }

        Map<TableId, List<TableId>> childParentMappings =
                PartitionMapper.resolveChildParents(parentTableIds, jdbc);
        Map<TableId, List<TableId>> parentToChildren =
                discovered == PartitionCaptureState.EMPTY
                        ? childParentMappings
                        : discovered.withUpdatedMappings(childParentMappings).getParentToChildren();
        if (!parentToChildren.isEmpty()
                && !parentToChildren.equals(discovered.getParentToChildren())) {
            discovered =
                    PartitionCaptureState.of(parentToChildren, true, partitionPublicationName());
            LOG.info(
                    "Resolved partition routing state from {} candidate table(s): "
                            + "{} parents with {} total children",
                    parentTableIds.size(),
                    parentToChildren.size(),
                    parentToChildren.values().stream().mapToInt(List::size).sum());
        }
        return discovered;
    }

    private String partitionPublicationName() {
        return getSlotName() != null
                ? sourceConfig.getDbzProperties().getProperty("publication.name", "dbz_publication")
                : null;
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            boolean includePartitionedTables =
                    ((PostgresSourceConfig) sourceConfig).includePartitionedTables();
            final List<TableId> capturedTableIds =
                    TableDiscoveryUtils.listTables(
                            // there is always a single database provided
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            includePartitionedTables);
            if (includePartitionedTables && !capturedTableIds.isEmpty()) {
                setCurrentPartitionState(discoverPartitionState(capturedTableIds, jdbc));
            }
            // fetch table schemas
            return queryTableSchema(jdbc, capturedTableIds);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new PostgresConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig);
        }
        TableChange result = schema.getTableSchema(tableId);
        return resolvePartitionParentSchema(jdbc, tableId, result);
    }

    private Map<TableId, TableChange> queryTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds) {
        if (schema == null) {
            schema = new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig);
        }
        Map<TableId, TableChange> tableSchemas = schema.getTableSchema(tableIds);
        if (sourceConfig.includePartitionedTables()) {
            // Collect the parents whose children are not yet known so we can do a single
            // batched discovery below instead of one round-trip per parent (O(N) -> O(1)).
            List<TableId> parentsToDiscover = new ArrayList<>();
            for (TableId tableId : tableIds) {
                TableChange tc = tableSchemas.get(tableId);
                if (needsChildResolution(tc)) {
                    parentsToDiscover.add(tableId);
                }
            }
            if (!parentsToDiscover.isEmpty()) {
                discoverChildPartitionsBatch(jdbc, parentsToDiscover);
            }
            for (TableId tableId : tableIds) {
                tableSchemas.computeIfPresent(
                        tableId, (id, tc) -> resolvePartitionParentSchema(jdbc, id, tc));
            }
        }
        return tableSchemas;
    }

    private static boolean needsChildResolution(TableChange tableSchema) {
        if (tableSchema == null || tableSchema.getTable() == null) {
            return false;
        }
        // Parents without PKs need a representative child to materialise the schema.
        return tableSchema.getTable().primaryKeyColumns().isEmpty();
    }

    /**
     * If the given table is a partitioned parent without primary keys, resolves the schema from a
     * representative child partition. PostgreSQL does not define PKs on the parent table, but child
     * partitions inherit the constraint. The split assigner needs PKs to generate snapshot splits.
     *
     * <p>Uses {@link #currentPartitionState} when available (streaming phase), otherwise performs a
     * one-time partition discovery for the given table.
     */
    private TableChange resolvePartitionParentSchema(
            JdbcConnection jdbc, TableId tableId, TableChange tableSchema) {
        if (!sourceConfig.includePartitionedTables()
                || tableSchema == null
                || tableSchema.getTable() == null
                || !tableSchema.getTable().primaryKeyColumns().isEmpty()) {
            return tableSchema;
        }

        // Check cache first
        Map<TableId, TableChange> schemaCache = getPartitionParentSchemaCache();
        TableChange cached = schemaCache.get(tableId);
        if (cached != null) {
            return cached;
        }

        // Get children from existing partition state, or discover if not yet available
        List<TableId> children =
                currentPartitionState
                        .getParentToChildren()
                        .getOrDefault(tableId, Collections.emptyList());
        if (children.isEmpty()) {
            children = discoverChildPartitions(jdbc, tableId);
        }

        // Find first child with PKs and use its schema as representative
        for (TableId childId : children) {
            TableChange childSchema = schema.getTableSchema(childId);
            if (childSchema != null
                    && childSchema.getTable() != null
                    && !childSchema.getTable().primaryKeyColumns().isEmpty()) {
                Table resolved = childSchema.getTable().edit().tableId(tableId).create();
                TableChange result =
                        new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, resolved);
                schemaCache.put(tableId, result);
                LOG.info("Resolved partition parent {} schema from child {}.", tableId, childId);
                return result;
            }
        }
        // Fallback: no child with PKs was found. Return the original (PK-less) parent schema
        // and emit a WARN so the operator knows the snapshot path will fall back to a full
        // table scan (no chunk-based parallel split can be generated). The capture state for
        // streaming is unaffected.
        LOG.warn(
                "Could not resolve partition parent {} schema: none of the {} discovered child "
                        + "partition(s) carries a primary key. Falling back to PK-less parent "
                        + "schema; snapshot splits will be table-scanned rather than chunked.",
                tableId,
                children.size());
        return tableSchema;
    }

    @VisibleForTesting
    Map<TableId, TableChange> getPartitionParentSchemaCache() {
        Map<TableId, TableChange> cache = partitionParentSchemaCache;
        if (cache == null) {
            synchronized (this) {
                cache = partitionParentSchemaCache;
                if (cache == null) {
                    cache = new ConcurrentHashMap<>();
                    partitionParentSchemaCache = cache;
                }
            }
        }
        return cache;
    }

    private List<TableId> discoverChildPartitions(JdbcConnection jdbc, TableId parentTableId) {
        try {
            PartitionCaptureState state =
                    TableDiscoveryUtils.discoverPartitionState(
                            Collections.singletonList(parentTableId),
                            jdbc,
                            true,
                            getSlotName() != null
                                    ? sourceConfig
                                            .getDbzProperties()
                                            .getProperty("publication.name", "dbz_publication")
                                    : null);
            return state.getParentToChildren().getOrDefault(parentTableId, Collections.emptyList());
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to discover child partitions for {}: {}",
                    parentTableId,
                    e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Batched variant of {@link #discoverChildPartitions(JdbcConnection, TableId)} that resolves
     * children for multiple parents in a single database round-trip. Used by the schema bulk-load
     * path to keep snapshot startup time linear in the number of tables, not quadratic in the
     * number of partitioned parents.
     */
    private void discoverChildPartitionsBatch(JdbcConnection jdbc, List<TableId> parents) {
        try {
            PartitionCaptureState discovered =
                    TableDiscoveryUtils.discoverPartitionState(
                            parents,
                            jdbc,
                            true,
                            getSlotName() != null
                                    ? sourceConfig
                                            .getDbzProperties()
                                            .getProperty("publication.name", "dbz_publication")
                                    : null);
            if (discovered == null || discovered == PartitionCaptureState.EMPTY) {
                return;
            }
            // Merge discovered partitions into currentPartitionState using withUpdatedMappings
            // to preserve any child partitions already discovered by the streaming phase.
            this.currentPartitionState =
                    currentPartitionState.withUpdatedMappings(discovered.getParentToChildren());
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to discover child partitions for {} parent(s): {}",
                    parents.size(),
                    e.getMessage());
        }
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            StreamSplit streamSplit = sourceSplitBase.asStreamSplit();
            // Only use PartitionAwareStreamFetchTask when partition routing is enabled AND the
            // startup mode includes a streaming phase. In snapshot-only mode (scan.startup.mode=
            // snapshot) the stream split is bounded and WAL consumption stops at the ending offset,
            // so dynamic partition discovery and publication management add unnecessary overhead.
            boolean needsPartitionAware =
                    sourceConfig.includePartitionedTables()
                            && !sourceConfig.getStartupOptions().isSnapshotOnly();
            if (needsPartitionAware) {
                this.streamFetchTask = new PartitionAwareStreamFetchTask(streamSplit);
            } else {
                initializeSnapshotOnlyPartitionState(streamSplit);
                this.streamFetchTask = new PostgresStreamFetchTask(streamSplit);
            }
            return this.streamFetchTask;
        }
    }

    private void initializeSnapshotOnlyPartitionState(StreamSplit streamSplit) {
        if (!sourceConfig.includePartitionedTables()
                || !sourceConfig.getStartupOptions().isSnapshotOnly()
                || !currentPartitionState.getChildToParent().isEmpty()) {
            return;
        }

        List<TableId> parentTables = snapshotOnlyPartitionParentCandidates(streamSplit);
        if (parentTables.isEmpty()) {
            return;
        }

        setCurrentPartitionState(discoverPartitionState(parentTables));
    }

    private List<TableId> snapshotOnlyPartitionParentCandidates(StreamSplit streamSplit) {
        LinkedHashSet<TableId> parentTables =
                new LinkedHashSet<>(streamSplit.getTableSchemas().keySet());
        if (parentTables.isEmpty()) {
            for (FinishedSnapshotSplitInfo splitInfo :
                    streamSplit.getFinishedSnapshotSplitInfos()) {
                parentTables.add(splitInfo.getTableId());
            }
        }
        return new ArrayList<>(parentTables);
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new PostgresSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }

        if (filters.isIncluded(tableId)) {
            return true;
        }

        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        if (!postgresSourceConfig.includePartitionedTables()) {
            return false;
        }

        TableId parentTable = currentPartitionState.getParentFor(tableId);
        if (parentTable != null) {
            return filters.isIncluded(parentTable);
        }

        return resolveIncludedPartitionChild(tableId);
    }

    private boolean resolveIncludedPartitionChild(TableId tableId) {
        if (getNonPartitionChildCache().contains(tableId)) {
            return false;
        }

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            PartitionCaptureState discovered =
                    discoverPartitionState(Collections.singletonList(tableId), jdbc);
            if (discovered == PartitionCaptureState.EMPTY
                    || discovered.getParentToChildren().isEmpty()) {
                getNonPartitionChildCache().add(tableId);
                return false;
            }

            PartitionCaptureState previous = currentPartitionState;
            setCurrentPartitionState(
                    previous == PartitionCaptureState.EMPTY || !previous.isRoutingEnabled()
                            ? discovered
                            : previous.withUpdatedMappings(discovered.getParentToChildren()));

            TableId parentTable = currentPartitionState.getParentFor(tableId);
            return parentTable != null && filters.isIncluded(parentTable);
        } catch (SQLException e) {
            if (isMissingRelation(e)) {
                LOG.debug(
                        "Partition parent resolution skipped for missing relation {}: {}",
                        tableId,
                        e.getMessage());
                return false;
            }
            throw new FlinkRuntimeException("Failed to resolve partition parent for " + tableId, e);
        }
    }

    private static boolean isMissingRelation(SQLException exception) {
        SQLException current = exception;
        while (current != null) {
            String sqlState = current.getSQLState();
            if ("42P01".equals(sqlState) || "3F000".equals(sqlState)) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        return sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name());
    }

    public boolean removeSlot(String slotName) {
        try (PostgresConnection jdbc = (PostgresConnection) openJdbcConnection(sourceConfig)) {
            return jdbc.dropReplicationSlot(slotName);
        } catch (Exception e) {
            return false;
        }
    }
}
