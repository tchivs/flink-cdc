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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;

/** The configuration for Postgres CDC source. */
public class PostgresSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final int lsnCommitCheckpointsDelay;
    private final boolean includePartitionedTables;
    private final boolean includeDatabaseInTableId;
    private Map<TableId, TableId> childToParentMapping;
    private Map<TableId, List<TableId>> parentToChildrenMapping;
    private boolean pg10PartitionMappingInitialized;

    public PostgresSourceConfig(
            int subtaskId,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst,
            boolean includePartitionedTables,
            boolean includeDatabaseInTableId) {
        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
        this.subtaskId = subtaskId;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
        this.includePartitionedTables = includePartitionedTables;
        this.includeDatabaseInTableId = includeDatabaseInTableId;
    }

    /**
     * Returns {@code subtaskId} value.
     *
     * @return subtask id
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    /**
     * Returns {@code lsnCommitCheckpointsDelay} value.
     *
     * @return lsn commit checkpoint delay
     */
    public int getLsnCommitCheckpointsDelay() {
        return this.lsnCommitCheckpointsDelay;
    }

    /**
     * Returns {@code includePartitionedTables} value.
     *
     * @return include partitioned table
     */
    public boolean includePartitionedTables() {
        return includePartitionedTables;
    }

    /**
     * Returns the slot name for backfill task.
     *
     * @return backfill task slot name
     */
    public String getSlotNameForBackfillTask() {
        return getDbzProperties().getProperty(SLOT_NAME.name()) + "_" + getSubtaskId();
    }

    /** Returns the JDBC URL for config unique key. */
    public String getJdbcUrl() {
        return String.format(
                "jdbc:postgresql://%s:%d/%s", getHostname(), getPort(), getDatabaseList().get(0));
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return new PostgresConnectorConfig(getDbzConfiguration());
    }

    /** Returns whether to include database in the generated Table ID. */
    public boolean isIncludeDatabaseInTableId() {
        return includeDatabaseInTableId;
    }

    /** Returns the child→parent partition mapping discovered for PG10. */
    public Map<TableId, TableId> getChildToParentMapping() {
        return childToParentMapping;
    }

    /** Returns the child→parent partition mapping, or an empty map if it is absent. */
    public Map<TableId, TableId> getChildToParentMappingOrEmpty() {
        return childToParentMapping == null ? Collections.emptyMap() : childToParentMapping;
    }

    /**
     * Sets the child→parent partition mapping.
     *
     * <p>Note: The provided map may be unmodifiable (e.g., from {@code Pg10CaptureState}). Callers
     * must not attempt to mutate the map after setting it.
     */
    public void setChildToParentMapping(Map<TableId, TableId> childToParentMapping) {
        this.childToParentMapping = childToParentMapping;
    }

    /**
     * Sets the parent→children partition mapping.
     *
     * <p>Note: The provided map may be unmodifiable (e.g., from {@code Pg10CaptureState}). Callers
     * must not attempt to mutate the map after setting it.
     */
    public void setParentToChildrenMapping(Map<TableId, List<TableId>> parentToChildrenMapping) {
        this.parentToChildrenMapping = parentToChildrenMapping;
    }

    /** Returns the parent→children partition mapping, or an empty map if it is absent. */
    public Map<TableId, List<TableId>> getParentToChildrenMappingOrEmpty() {
        return parentToChildrenMapping == null ? Collections.emptyMap() : parentToChildrenMapping;
    }

    /** Returns whether the PG10 partition mapping has been initialized. */
    public boolean isPg10PartitionMappingInitialized() {
        return pg10PartitionMappingInitialized;
    }

    /** Sets whether the PG10 partition mapping has been initialized. */
    public void setPg10PartitionMappingInitialized(boolean pg10PartitionMappingInitialized) {
        this.pg10PartitionMappingInitialized = pg10PartitionMappingInitialized;
    }
}
