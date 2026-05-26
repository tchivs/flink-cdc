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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10CaptureState;
import org.apache.flink.cdc.connectors.postgres.source.fetch.Pg10StreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresScanFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionMapper;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PartitionReconciler;
import org.apache.flink.cdc.connectors.postgres.source.utils.Pg10PublicationManager;
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
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.TopicSelector;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;
import static io.debezium.connector.postgresql.Utils.currentOffset;

/** The dialect for Postgres. */
public class PostgresDialect implements JdbcDataSourceDialect {
    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "postgres-cdc-connector";

    private final PostgresSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    @Nullable private PostgresStreamFetchTask streamFetchTask;
    private transient Pg10PartitionReconciler pg10PartitionReconciler;

    public PostgresDialect(PostgresSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    private Pg10PartitionReconciler getPg10PartitionReconciler() {
        if (pg10PartitionReconciler == null) {
            pg10PartitionReconciler = new Pg10PartitionReconciler();
        }
        return pg10PartitionReconciler;
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
            PostgresSourceConfig pgConfig = (PostgresSourceConfig) sourceConfig;
            boolean includePartitionedTables = pgConfig.includePartitionedTables();
            List<TableId> discoveredTables =
                    TableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            includePartitionedTables);

            if (includePartitionedTables && isPg10(jdbc)) {
                discoverAndValidatePartitionMappings(jdbc, pgConfig, discoveredTables);
                synchronizeDialectPg10PartitionMappings(pgConfig);
            }

            return discoveredTables;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    private boolean isPg10(JdbcConnection jdbc) throws SQLException {
        int majorVersion = jdbc.connection().getMetaData().getDatabaseMajorVersion();
        return majorVersion == 10;
    }

    private void discoverAndValidatePartitionMappings(
            JdbcConnection jdbc, PostgresSourceConfig pgConfig, List<TableId> parentTables)
            throws SQLException {
        if (pgConfig.isPg10PartitionMappingInitialized()) {
            return;
        }

        Map<TableId, List<TableId>> parentToChildren =
                TableDiscoveryUtils.discoverPartitionedTableMappings(parentTables, jdbc, true);
        pgConfig.setParentToChildrenMapping(parentToChildren);

        if (!parentToChildren.isEmpty()) {
            Map<TableId, TableId> childToParent =
                    Pg10PartitionMapper.buildChildToParentMapping(parentToChildren);
            pgConfig.setChildToParentMapping(childToParent);

            String publicationName = pgConfig.getDbzProperties().getProperty("publication.name");
            if (publicationName != null) {
                List<TableId> allChildren =
                        Pg10PartitionMapper.getAllChildTableIds(parentToChildren);
                Pg10PartitionMapper.validatePublicationMembership(
                        jdbc, publicationName, allChildren);
            }
        }

        pgConfig.setPg10PartitionMappingInitialized(true);
    }

    private void synchronizeDialectPg10PartitionMappings(PostgresSourceConfig pgConfig) {
        if (pgConfig == sourceConfig || !pgConfig.isPg10PartitionMappingInitialized()) {
            return;
        }

        sourceConfig.setChildToParentMapping(pgConfig.getChildToParentMapping());
        sourceConfig.setParentToChildrenMapping(pgConfig.getParentToChildrenMappingOrEmpty());
        sourceConfig.setPg10PartitionMappingInitialized(true);
    }

    public void syncPg10PartitionMappings(
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        sourceConfig.setChildToParentMapping(childToParentMapping);
        sourceConfig.setParentToChildrenMapping(parentToChildrenMapping);
        sourceConfig.setPg10PartitionMappingInitialized(true);
    }

    /**
     * Reconciles PG10 partition mappings using the pure {@link Pg10PartitionReconciler}.
     *
     * <p>This method is side-effect free - it only reads the catalog and returns the diff.
     */
    public Pg10PartitionReconciler.Pg10ReconcileResult reconcilePg10PartitionMappings(
            JdbcConnection jdbc, PostgresSourceConfig pgConfig, List<TableId> parentTables)
            throws SQLException {
        return getPg10PartitionReconciler()
                .reconcile(
                        jdbc,
                        parentTables,
                        pgConfig.getParentToChildrenMappingOrEmpty(),
                        pgConfig.getChildToParentMappingOrEmpty());
    }

    public Pg10PartitionReconciler.Pg10ReconcileResult reconcilePg10PartitionMappings(
            JdbcConnection jdbc, Pg10CaptureState captureState, List<TableId> parentTables)
            throws SQLException {
        return getPg10PartitionReconciler()
                .reconcile(
                        jdbc,
                        parentTables,
                        captureState.getParentToChildrenMapping(),
                        captureState.getChildToParentMapping());
    }

    public void validateRestoredPg10CaptureState(
            JdbcConnection jdbc, Pg10CaptureState restoredCaptureState, List<TableId> parentTables)
            throws SQLException {
        Pg10PartitionReconciler.Pg10ReconcileResult reconcileResult =
                getPg10PartitionReconciler()
                        .reconcile(
                                jdbc,
                                parentTables,
                                restoredCaptureState.getParentToChildrenMapping(),
                                restoredCaptureState.getChildToParentMapping());

        List<String> validationIssues = new ArrayList<>();
        List<TableId> publicationValidatedChildren = new ArrayList<>();
        Map<TableId, TableId> latestChildToParent = reconcileResult.getLatestChildToParent();
        for (Map.Entry<TableId, TableId> entry :
                restoredCaptureState.getChildToParentMapping().entrySet()) {
            TableId childTable = entry.getKey();
            TableId restoredParentTable = entry.getValue();
            TableId latestParentTable = latestChildToParent.get(childTable);
            if (latestParentTable == null) {
                validationIssues.add(
                        String.format(
                                "child '%s' is missing from the current catalog (restored parent: '%s')",
                                childTable, restoredParentTable));
            } else if (!restoredParentTable.equals(latestParentTable)) {
                validationIssues.add(
                        String.format(
                                "child '%s' is now mapped to parent '%s' instead of restored parent '%s'",
                                childTable, latestParentTable, restoredParentTable));
            } else {
                publicationValidatedChildren.add(childTable);
            }
        }

        String publicationName = sourceConfig.getDbzProperties().getProperty("publication.name");
        if (publicationName != null) {
            List<TableId> missingPublicationChildren =
                    Pg10PublicationManager.findMissingPublicationChildren(
                            jdbc, publicationName, publicationValidatedChildren);
            for (TableId childTable : missingPublicationChildren) {
                validationIssues.add(
                        String.format(
                                "child '%s' is no longer a member of publication '%s'",
                                childTable, publicationName));
            }
        }

        if (!validationIssues.isEmpty()) {
            throw new FlinkRuntimeException(
                    "Restored PG10 routing state references missing or altered child partitions: "
                            + String.join("; ", validationIssues));
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = queryTableSchema(jdbc, capturedTableIds);
            return tableSchemas;
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
        return new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig)
                .getTableSchema(tableId);
    }

    private Map<TableId, TableChange> queryTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds) {
        return new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig)
                .getTableSchema(tableIds);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = createStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    private PostgresStreamFetchTask createStreamFetchTask(StreamSplit streamSplit) {
        if (sourceConfig.isPg10PartitionMappingInitialized()
                || streamSplit.isPg10RoutingStateInitialized()) {
            return new Pg10StreamFetchTask(streamSplit);
        }
        return new PostgresStreamFetchTask(streamSplit);
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

        return filters.isIncluded(tableId);
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
