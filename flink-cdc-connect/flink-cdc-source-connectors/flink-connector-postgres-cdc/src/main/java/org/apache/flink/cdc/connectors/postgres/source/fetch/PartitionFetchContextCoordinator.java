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

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/**
 * Coordinates partition-aware capture-state and runtime operations for streaming fetch tasks.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Building session-scoped {@link StreamingSessionRuntime} instances with fresh Debezium
 *       objects on each session restart.
 *   <li>Deriving parent table IDs from the stream split and capture state.
 *   <li>Reconciling capture state when new child partitions are discovered.
 * </ul>
 */
final class PartitionFetchContextCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionFetchContextCoordinator.class);

    private final PostgresSourceFetchTaskContext context;

    PartitionFetchContextCoordinator(PostgresSourceFetchTaskContext context) {
        this.context = context;
    }

    /**
     * Builds a fresh {@link StreamingSessionRuntime} with all session-scoped Debezium objects. Each
     * session restart creates a new runtime instance.
     */
    StreamingSessionRuntime buildStreamingRuntime(
            StreamSplit streamSplit,
            PartitionCaptureState captureState,
            PostgresOffsetContext startingOffsetContext,
            BiConsumer<TableId, TableId> partitionRouteListener) {
        PostgresConnectorConfig dbzConfig = buildPartitionAwareDbzConfig(streamSplit, captureState);

        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        dbzConfig.getConfig().getString(SNAPSHOT_MODE));
        Snapshotter snapShotter = snapshotMode.getSnapshotter(dbzConfig.getConfig());

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbcConnection =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        "postgres-partition-session");

        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);

        RelationAwarePostgresSchema schema;
        try {
            schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresSchema for session", e);
        }

        PostgresPartition partition = new PostgresPartition(dbzConfig.getLogicalName());
        PostgresTaskContext taskContext =
                PostgresObjectUtils.newTaskContext(dbzConfig, schema, topicSelector);

        ReplicationConnection replicationConnection =
                context.createReplicationConnection(
                        taskContext, jdbcConnection, snapShotter.shouldSnapshot(), dbzConfig);

        ErrorHandler errorHandler = new PostgresErrorHandler(dbzConfig, context.getQueue());
        EventMetadataProvider metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        CDCPostgresDispatcher dispatcher =
                context.createDispatcher(
                        dbzConfig,
                        topicSelector,
                        schema,
                        jdbcConnection,
                        context.getQueue(),
                        metadataProvider,
                        captureState.getChildToParent(),
                        partitionRouteListener);
        schema.setDispatcher(dispatcher);
        schema.setPartitionRoutingEnabled(true);

        return new StreamingSessionRuntime(
                dbzConfig,
                jdbcConnection,
                replicationConnection,
                schema,
                dispatcher,
                errorHandler,
                taskContext,
                partition,
                snapShotter,
                startingOffsetContext,
                context.getQueue(),
                captureState);
    }

    private PostgresConnectorConfig buildPartitionAwareDbzConfig(
            StreamSplit streamSplit, PartitionCaptureState captureState) {
        PostgresConnectorConfig dbzConfig = context.getDbzConnectorConfig();
        if (!shouldExpandTableIncludeListForPartitions(dbzConfig, captureState)) {
            return dbzConfig;
        }

        // Both partitioned parents and their child partitions are intentionally added to
        // table.include.list:
        //   - Children must be included so Debezium captures their WAL events.
        //   - Parents must be included so the table filter accepts events that
        //     CDCPostgresDispatcher#preRouteTableId has rewritten from a child id to the
        //     parent id before dispatch.
        // PostgreSQL itself rejects partitioned parents in a publication, but that is
        // handled at publication-creation time by
        // PostgresReplicationConnection#determineCapturedTables, which strips parents
        // (relkind = 'p') from the FOR TABLE list. Do not remove parents here.
        List<TableId> includedTables = new ArrayList<>();
        includedTables.addAll(deriveParentTables(streamSplit, captureState));
        includedTables.addAll(captureState.getChildToParent().keySet());
        if (includedTables.isEmpty()) {
            return dbzConfig;
        }

        Configuration.Builder builder = dbzConfig.getConfig().edit();
        builder.with("table.include.list", tableIncludeList(includedTables));
        return new PostgresConnectorConfig(builder.build());
    }

    private boolean shouldExpandTableIncludeListForPartitions(
            PostgresConnectorConfig dbzConfig, PartitionCaptureState captureState) {
        if (captureState == null
                || !captureState.isRoutingEnabled()
                || captureState.getChildToParent().isEmpty()) {
            return false;
        }
        String autocreateModeValue = dbzConfig.getConfig().getString("publication.autocreate.mode");
        PostgresConnectorConfig.AutoCreateMode autocreateMode =
                PostgresConnectorConfig.AutoCreateMode.parse(
                        autocreateModeValue == null ? "all_tables" : autocreateModeValue,
                        "all_tables");
        return autocreateMode == PostgresConnectorConfig.AutoCreateMode.FILTERED;
    }

    private String tableIncludeList(List<TableId> tableIds) {
        List<String> names = new ArrayList<>();
        for (TableId tableId : tableIds) {
            if (tableId.schema() == null || tableId.schema().isEmpty()) {
                names.add(tableId.table());
            } else {
                names.add(tableId.schema() + "." + tableId.table());
            }
        }
        return String.join(",", names);
    }

    /** Closes a streaming runtime, releasing all session-scoped resources. */
    void closeStreamingRuntime(StreamingSessionRuntime runtime) {
        runtime.close();
    }

    /**
     * Builds the initial {@link PartitionCaptureState} by discovering partition mappings via {@link
     * PostgresDialect#discoverPartitionState}.
     */
    PartitionCaptureState buildInitialCaptureState(StreamSplit streamSplit) {
        PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) context.getSourceConfig();
        if (!pgSourceConfig.includePartitionedTables()) {
            return PartitionCaptureState.EMPTY;
        }
        List<TableId> parentTables = deriveParentTables(streamSplit, PartitionCaptureState.EMPTY);
        if (parentTables.isEmpty()) {
            return PartitionCaptureState.EMPTY;
        }
        PostgresDialect dialect = (PostgresDialect) context.getDataSourceDialect();
        return dialect.discoverPartitionState(parentTables);
    }

    /**
     * Reconciles partition mappings by re-querying pg_inherits, returning an updated capture state
     * if new children were discovered.
     */
    PartitionCaptureState reconcilePartitionMappings(
            PartitionCaptureState currentState, List<TableId> parentTables) {
        if (parentTables.isEmpty()) {
            return currentState;
        }

        try (PostgresConnection jdbc =
                new PostgresConnection(
                        context.getDbzConnectorConfig().getJdbcConfig(),
                        newPostgresValueConverterBuilder(context.getDbzConnectorConfig()),
                        "postgres-partition-reconcile")) {
            Map<TableId, List<TableId>> latestParentToChildren =
                    PartitionMapper.discoverPartitionMappings(parentTables, jdbc, true);
            Map<TableId, TableId> latestChildToParent =
                    PartitionMapper.buildChildToParentMapping(latestParentToChildren);

            if (latestChildToParent.equals(currentState.getChildToParent())) {
                return currentState;
            }

            LOG.info(
                    "Partition reconciliation detected changes: previous children={}, latest children={}",
                    currentState.getChildToParent().size(),
                    latestChildToParent.size());
            PostgresSourceConfig pgSourceConfig = (PostgresSourceConfig) context.getSourceConfig();
            String publicationName =
                    PartitionAwareStreamFetchTask.resolvePublicationName(pgSourceConfig);
            return PartitionCaptureState.of(latestParentToChildren, true, publicationName);
        } catch (SQLException e) {
            // Transient JDBC errors (connectivity / lock contention / query timeout) keep the
            // existing capture state so the streaming session can continue. The next session
            // restart triggered by a new Relation message will re-attempt reconciliation.
            LOG.warn(
                    "Failed to reconcile partition mappings due to a JDBC error; keeping current state.",
                    e);
            return currentState;
        }
        // RuntimeException (NPE / IllegalState / programming errors) is intentionally not caught
        // here — those indicate a structural problem and must surface to the streaming task so
        // the failure is visible instead of silently leaving the routing state stale.
    }

    /**
     * Incrementally reconciles partition mappings by querying pg_inherits only for specific unknown
     * child tables detected in the WAL stream. This is O(unknown_count) instead of scanning all
     * children of all parent tables, making it significantly more efficient when a large number of
     * partitions exist and only a few new ones are detected.
     *
     * @param currentState the current partition capture state
     * @param unknownRelations the specific child TableIds detected in WAL that are not in the
     *     current capture state
     * @return updated capture state with new children merged in, or currentState if no changes
     */
    IncrementalReconciliationResult incrementalReconcile(
            PartitionCaptureState currentState,
            Set<TableId> unknownRelations,
            Collection<TableId> monitoredParentTables) {
        if (unknownRelations == null || unknownRelations.isEmpty()) {
            return new IncrementalReconciliationResult(currentState, Collections.emptySet());
        }

        try (PostgresConnection jdbc =
                new PostgresConnection(
                        context.getDbzConnectorConfig().getJdbcConfig(),
                        newPostgresValueConverterBuilder(context.getDbzConnectorConfig()),
                        "postgres-partition-incremental-reconcile")) {
            // Resolve parents only for the unknown child tables — O(unknown_count) query
            Map<TableId, List<TableId>> newParentToChildren =
                    PartitionMapper.resolveChildParents(unknownRelations, jdbc);
            FilteredPartitionResolution filteredResolution =
                    filterResolvedPartitions(
                            newParentToChildren, unknownRelations, monitoredParentTables);
            if (filteredResolution.monitoredParentToChildren.isEmpty()) {
                LOG.info(
                        "Incremental reconciliation: none of the {} unknown relation(s) "
                                + "are child partitions of monitored parents.",
                        unknownRelations.size());
                return new IncrementalReconciliationResult(
                        currentState, filteredResolution.unrelatedRelations);
            }

            // Merge newly discovered children into the existing state
            PartitionCaptureState merged =
                    currentState.withUpdatedMappings(filteredResolution.monitoredParentToChildren);
            if (merged.equals(currentState)) {
                return new IncrementalReconciliationResult(
                        currentState, filteredResolution.unrelatedRelations);
            }

            LOG.info(
                    "Incremental reconciliation discovered {} new child partition(s) "
                            + "from {} unknown relation(s). Total children: {}",
                    filteredResolution.monitoredParentToChildren.values().stream()
                            .mapToInt(List::size)
                            .sum(),
                    unknownRelations.size(),
                    merged.getChildToParent().size());
            return new IncrementalReconciliationResult(
                    merged, filteredResolution.unrelatedRelations);
        } catch (SQLException e) {
            // Transient JDBC errors — fall back to full reconciliation
            LOG.warn(
                    "Incremental reconciliation failed due to JDBC error; "
                            + "falling back to full reconciliation.",
                    e);
            List<TableId> parentTables =
                    monitoredParentTables == null
                            ? new ArrayList<>()
                            : new ArrayList<>(monitoredParentTables);
            return new IncrementalReconciliationResult(
                    reconcilePartitionMappings(currentState, parentTables), Collections.emptySet());
        }
    }

    static FilteredPartitionResolution filterResolvedPartitions(
            Map<TableId, List<TableId>> resolvedParentToChildren,
            Set<TableId> unknownRelations,
            Collection<TableId> monitoredParentTables) {
        Set<TableId> monitoredParents =
                monitoredParentTables == null
                        ? Collections.emptySet()
                        : new HashSet<>(monitoredParentTables);
        Map<TableId, List<TableId>> monitoredParentToChildren = new LinkedHashMap<>();
        Set<TableId> acceptedChildren = new HashSet<>();
        Set<TableId> rejectedChildren = new LinkedHashSet<>();

        for (Map.Entry<TableId, List<TableId>> entry : resolvedParentToChildren.entrySet()) {
            if (monitoredParents.contains(entry.getKey())) {
                monitoredParentToChildren.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                acceptedChildren.addAll(entry.getValue());
            } else {
                rejectedChildren.addAll(entry.getValue());
            }
        }

        Set<TableId> unresolvedRelations = new LinkedHashSet<>(unknownRelations);
        unresolvedRelations.removeAll(acceptedChildren);
        unresolvedRelations.removeAll(rejectedChildren);
        rejectedChildren.addAll(unresolvedRelations);
        rejectedChildren.removeAll(acceptedChildren);

        return new FilteredPartitionResolution(monitoredParentToChildren, rejectedChildren);
    }

    static final class IncrementalReconciliationResult {
        private final PartitionCaptureState captureState;
        private final Set<TableId> unrelatedRelations;

        private IncrementalReconciliationResult(
                PartitionCaptureState captureState, Set<TableId> unrelatedRelations) {
            this.captureState = captureState;
            this.unrelatedRelations =
                    Collections.unmodifiableSet(new LinkedHashSet<>(unrelatedRelations));
        }

        PartitionCaptureState getCaptureState() {
            return captureState;
        }

        Set<TableId> getUnrelatedRelations() {
            return unrelatedRelations;
        }
    }

    static final class FilteredPartitionResolution {
        private final Map<TableId, List<TableId>> monitoredParentToChildren;
        private final Set<TableId> unrelatedRelations;

        private FilteredPartitionResolution(
                Map<TableId, List<TableId>> monitoredParentToChildren,
                Set<TableId> unrelatedRelations) {
            this.monitoredParentToChildren = monitoredParentToChildren;
            this.unrelatedRelations = unrelatedRelations;
        }

        Map<TableId, List<TableId>> getMonitoredParentToChildren() {
            return monitoredParentToChildren;
        }

        Set<TableId> getUnrelatedRelations() {
            return unrelatedRelations;
        }
    }

    /**
     * Derives the list of parent (partitioned) table IDs from the stream split's table schemas,
     * excluding known child partitions.
     */
    static List<TableId> deriveParentTables(
            StreamSplit streamSplit, PartitionCaptureState captureState) {
        LinkedHashMap<TableId, TableId> knownChildToParent = new LinkedHashMap<>();
        if (captureState != null) {
            knownChildToParent.putAll(captureState.getChildToParent());
        }

        List<TableId> parentTables = new ArrayList<>();
        for (TableId tableId : streamSplit.getTableSchemas().keySet()) {
            if (!knownChildToParent.containsKey(tableId)) {
                parentTables.add(tableId);
            }
        }

        if (parentTables.isEmpty() && captureState != null) {
            parentTables.addAll(captureState.getParentToChildren().keySet());
        }
        return parentTables;
    }
}
