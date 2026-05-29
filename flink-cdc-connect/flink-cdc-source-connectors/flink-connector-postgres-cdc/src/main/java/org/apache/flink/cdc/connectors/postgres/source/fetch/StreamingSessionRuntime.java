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

import org.apache.flink.cdc.connectors.postgres.source.schema.RelationAwarePostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PartitionCaptureState;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime container for Debezium session-scoped objects in partition-aware streaming mode.
 *
 * <p>This class encapsulates all session-scoped resources that need to be built together and closed
 * as a unit when a streaming session is restarted for partition reconciliation. It holds references
 * to the JDBC connection, replication connection, schema, dispatcher, error handler, and other
 * components required for streaming CDC.
 */
public final class StreamingSessionRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingSessionRuntime.class);

    private final PostgresConnectorConfig dbzConnectorConfig;
    private final PostgresConnection jdbcConnection;
    private final ReplicationConnection replicationConnection;
    private final RelationAwarePostgresSchema schema;
    private final CDCPostgresDispatcher dispatcher;
    private final ErrorHandler errorHandler;
    private final PostgresTaskContext taskContext;
    private final PostgresPartition partition;
    private final Snapshotter snapShotter;
    private final PostgresOffsetContext offsetContext;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final PartitionCaptureState captureState;

    public StreamingSessionRuntime(
            PostgresConnectorConfig dbzConnectorConfig,
            PostgresConnection jdbcConnection,
            ReplicationConnection replicationConnection,
            RelationAwarePostgresSchema schema,
            CDCPostgresDispatcher dispatcher,
            ErrorHandler errorHandler,
            PostgresTaskContext taskContext,
            PostgresPartition partition,
            Snapshotter snapShotter,
            PostgresOffsetContext offsetContext,
            ChangeEventQueue<DataChangeEvent> queue,
            PartitionCaptureState captureState) {
        this.dbzConnectorConfig = dbzConnectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.replicationConnection = replicationConnection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.partition = partition;
        this.snapShotter = snapShotter;
        this.offsetContext = offsetContext;
        this.queue = queue;
        this.captureState = captureState;
    }

    public PostgresConnectorConfig getDbzConnectorConfig() {
        return dbzConnectorConfig;
    }

    public PostgresConnection getJdbcConnection() {
        return jdbcConnection;
    }

    public ReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    public RelationAwarePostgresSchema getSchema() {
        return schema;
    }

    public CDCPostgresDispatcher getDispatcher() {
        return dispatcher;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    public PostgresPartition getPartition() {
        return partition;
    }

    public Snapshotter getSnapShotter() {
        return snapShotter;
    }

    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    public PartitionCaptureState getCaptureState() {
        return captureState;
    }

    @Override
    public void close() {
        closeJdbcConnectionQuietly();
        closeReplicationConnectionQuietly();
    }

    private void closeJdbcConnectionQuietly() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close JDBC connection during session runtime close", e);
        }
    }

    private void closeReplicationConnectionQuietly() {
        try {
            if (replicationConnection != null) {
                replicationConnection.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close replication connection during session runtime close", e);
        }
    }
}
