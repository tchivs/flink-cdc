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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
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
            PostgresOffsetContext startingOffsetContext) {
        PostgresConnectorConfig dbzConfig = context.getDbzConnectorConfig();

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
                createReplicationConnection(
                        taskContext, jdbcConnection, snapShotter.shouldSnapshot(), dbzConfig);

        ErrorHandler errorHandler = new PostgresErrorHandler(dbzConfig, context.getQueue());
        EventMetadataProvider metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        CDCPostgresDispatcher dispatcher =
                context.createDispatcher(
                        dbzConfig,
                        topicSelector,
                        schema,
                        context.getQueue(),
                        metadataProvider,
                        captureState.getChildToParent());
        schema.setDispatcher(dispatcher);

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
                    pgSourceConfig.getDbzProperties().getProperty("publication.name");
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
