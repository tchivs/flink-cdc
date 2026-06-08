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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.Objects;
import java.util.function.Supplier;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.data.Envelope.FieldName.SOURCE;

/** Routes physical PostgreSQL partition child table IDs to logical parent table IDs. */
public final class PostgresTableIdRouter {

    public static final String ERR_PR_001 = "ERR-PR-001";

    private static final PostgresTableIdRouter EMPTY =
            new PostgresTableIdRouter(() -> PartitionRoutingState.EMPTY);

    private final Supplier<PartitionRoutingState> routingStateSupplier;

    private PostgresTableIdRouter(Supplier<PartitionRoutingState> routingStateSupplier) {
        this.routingStateSupplier = routingStateSupplier;
    }

    public static PostgresTableIdRouter empty() {
        return EMPTY;
    }

    public static PostgresTableIdRouter of(Supplier<PartitionRoutingState> routingStateSupplier) {
        return new PostgresTableIdRouter(
                routingStateSupplier == null
                        ? () -> PartitionRoutingState.EMPTY
                        : routingStateSupplier);
    }

    public TableId route(TableId tableId) {
        if (tableId == null) {
            return null;
        }
        PartitionRoutingState routingState = routingStateSupplier.get();
        if (routingState == null || routingState.isEmpty()) {
            return tableId;
        }
        return routingState.routeToLogicalTable(tableId);
    }

    public boolean hasRoutingState() {
        PartitionRoutingState routingState = routingStateSupplier.get();
        return routingState != null && !routingState.isEmpty();
    }

    public boolean isKnownChild(TableId tableId) {
        PartitionRoutingState routingState = routingStateSupplier.get();
        return routingState != null && routingState.containsChild(tableId);
    }

    public Table route(Table table) {
        if (table == null) {
            return null;
        }
        TableId routedTableId = route(table.id());
        if (routedTableId == null || routedTableId.equals(table.id())) {
            return table;
        }
        return table.edit().tableId(routedTableId).create();
    }

    public void validateKnownChildForDecoderbufs(
            TableId tableId,
            String decoderName,
            StartupOptions startupOptions,
            TableId resolvedParent) {
        if (!"decoderbufs".equalsIgnoreCase(decoderName)
                || tableId == null
                || resolvedParent == null) {
            return;
        }
        PartitionRoutingState routingState = routingStateSupplier.get();
        if (routingState == null || routingState.isEmpty() || routingState.containsChild(tableId)) {
            return;
        }
        if (!routingState.containsParent(resolvedParent)) {
            return;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] Unsupported runtime child discovery: decoding.plugin.name=%s, "
                                + "scan.startup.mode=%s, partition parent=%s, unknown child=%s. "
                                + "Reason: decoderbufs supports only static partition routing for "
                                + "children discovered during startup seed. Runtime new child "
                                + "partitions cannot be discovered with pgoutput Relation-message "
                                + "semantics. Support level: limited/static. Remediation: switch "
                                + "decoding.plugin.name=pgoutput, or restart the job after adding "
                                + "the child partition so catalog seed can include %s.",
                        ERR_PR_001,
                        decoderName,
                        startupOptions.startupMode,
                        resolvedParent,
                        tableId,
                        tableId));
    }

    public void rewriteSourceStruct(Struct value) {
        if (value == null) {
            return;
        }
        Struct source = value.getStruct(SOURCE);
        if (source == null || source.schema().field(TABLE_NAME_KEY) == null) {
            return;
        }
        Field schemaField = source.schema().field(SCHEMA_NAME_KEY);
        String schemaName = schemaField == null ? null : source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        if (tableName == null) {
            return;
        }
        TableId routedTableId = route(new TableId(null, schemaName, tableName));
        if (routedTableId == null
                || (Objects.equals(schemaName, routedTableId.schema())
                        && tableName.equals(routedTableId.table()))) {
            return;
        }
        if (schemaField != null) {
            source.put(SCHEMA_NAME_KEY, routedTableId.schema());
        }
        source.put(TABLE_NAME_KEY, routedTableId.table());
    }
}
