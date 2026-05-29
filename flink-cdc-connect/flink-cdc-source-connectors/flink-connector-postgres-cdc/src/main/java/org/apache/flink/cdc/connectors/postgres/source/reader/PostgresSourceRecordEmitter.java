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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;

/**
 * Record emitter that recognizes {@link PostgresSchemaRecord} as schema change events and applies
 * child→parent partition routing to both data and schema records.
 */
public class PostgresSourceRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final Supplier<Map<TableId, TableId>> childToParentMappingSupplier;

    public PostgresSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            Supplier<Map<TableId, TableId>> childToParentMappingSupplier) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                includeSchemaChanges,
                offsetFactory);
        this.childToParentMappingSupplier =
                childToParentMappingSupplier == null
                        ? Collections::emptyMap
                        : childToParentMappingSupplier;
    }

    /** Returns current mapping, defensively guarding against suppliers that return null. */
    protected Map<TableId, TableId> getChildToParentMapping() {
        Map<TableId, TableId> mapping = childToParentMappingSupplier.get();
        return mapping == null ? Collections.emptyMap() : mapping;
    }

    protected TableId routeTableId(TableId tableId) {
        TableId parentTableId = getChildToParentMapping().get(tableId);
        return parentTableId == null ? tableId : parentTableId;
    }

    @Override
    protected TableChanges getTableChangeRecord(SourceRecord element) throws IOException {
        if (element instanceof PostgresSchemaRecord) {
            PostgresSchemaRecord schemaRecord = (PostgresSchemaRecord) element;
            Table table = routeTable(schemaRecord.getTable());
            return new TableChanges().create(table);
        } else {
            return super.getTableChangeRecord(element);
        }
    }

    private Table routeTable(Table table) {
        TableId routedTableId = routeTableId(table.id());
        if (routedTableId.equals(table.id())) {
            return table;
        }
        return table.edit().tableId(routedTableId).create();
    }

    @Override
    protected void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        SourceRecord routedRecord = element;
        if (isDataChangeRecord(element)) {
            routedRecord = rewriteDataRecordTableId(element);
        }
        super.emitElement(routedRecord, output);
    }

    /**
     * Rewrites the table identifier in a data change record's source struct to reflect the routed
     * parent table. This performs an <b>in-place mutation</b> of the Kafka Connect Struct, which is
     * safe because SourceRecords are consumed by a single-threaded reader pipeline and are never
     * shared across consumers.
     */
    private SourceRecord rewriteDataRecordTableId(SourceRecord element) {
        Object valueObj = element.value();
        if (!(valueObj instanceof Struct)) {
            return element;
        }

        Struct value = (Struct) valueObj;
        Struct source = value.getStruct(SOURCE);
        if (source == null) {
            return element;
        }

        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        if (schemaName == null || tableName == null) {
            return element;
        }

        TableId currentTableId = new TableId(null, schemaName, tableName);
        TableId routedTableId = routeTableId(currentTableId);
        if (routedTableId.equals(currentTableId)) {
            return element;
        }

        source.put(SCHEMA_NAME_KEY, routedTableId.schema());
        source.put(TABLE_NAME_KEY, routedTableId.table());
        return element;
    }
}
