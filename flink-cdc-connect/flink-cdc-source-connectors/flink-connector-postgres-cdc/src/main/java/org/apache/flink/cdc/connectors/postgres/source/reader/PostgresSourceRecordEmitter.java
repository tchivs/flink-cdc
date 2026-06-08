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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;

/**
 * Record emitter that recognizes {@link PostgresSchemaRecord} as schema change events.
 *
 * <h2>Why child→parent partition routing is upstream, not here</h2>
 *
 * <p>Routing of child partition tableIds to their parent is split across two pre-dispatch entry
 * points so that the emitter itself stays schema-routing-agnostic:
 *
 * <ul>
 *   <li><b>Data change events</b> (DML): the WAL thread (in {@code
 *       io.debezium.connector.postgresql.PostgresStreamingChangeEventSource}) calls {@code
 *       CDCPostgresDispatcher.preRouteTableId()} BEFORE constructing this emitter's record. The
 *       emitter therefore sees a SourceRecord whose source struct already carries the parent
 *       tableId; no further work is required here.
 *   <li><b>Schema change events</b>: {@code CDCPostgresDispatcher.dispatch(Table)} routes the
 *       schema events for the parent. The dispatched {@link PostgresSchemaRecord} is produced from
 *       the parent table so {@link #getTableChangeRecord(SourceRecord)} can simply unwrap it
 *       without re-routing.
 * </ul>
 *
 * <p>This split keeps the emitter free of partition state, removes a prior child→parent rewrite
 * that was both expensive and order-dependent, and ensures both schema and data events flow
 * downstream under a single consistent parent tableId.
 */
public class PostgresSourceRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    public PostgresSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                includeSchemaChanges,
                offsetFactory);
    }

    @Override
    protected TableChanges getTableChangeRecord(SourceRecord element) throws IOException {
        if (element instanceof PostgresSchemaRecord) {
            // PostgresSchemaRecord is built from the routed (parent) table inside
            // CDCPostgresDispatcher.dispatch(Table); see class-level Javadoc for the
            // schema/data dual-entry-point design. Unwrap directly without re-routing.
            PostgresSchemaRecord schemaRecord = (PostgresSchemaRecord) element;
            return new TableChanges().create(schemaRecord.getTable());
        } else {
            return super.getTableChangeRecord(element);
        }
    }
}
