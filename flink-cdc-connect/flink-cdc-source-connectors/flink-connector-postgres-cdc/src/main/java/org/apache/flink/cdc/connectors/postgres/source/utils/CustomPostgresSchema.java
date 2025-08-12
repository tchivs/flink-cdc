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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;

import java.sql.SQLException;
import java.util.List;

/**
 * A CustomPostgresSchema optimized for PostgreSQL versions 11 and above. This version uses standard
 * schema reading without special partition handling. For PostgreSQL 10, use CustomPostgresSchemaV10
 * instead.
 */
public class CustomPostgresSchema extends AbstractCustomPostgresSchema {

    public CustomPostgresSchema(
            PostgresConnection jdbcConnection, PostgresSourceConfig sourceConfig) {
        super(jdbcConnection, sourceConfig);
    }

    @Override
    protected void readSchemaWithVersionSpecificHandling(
            Tables tables, List<TableId> targetTableIds) throws SQLException {
        // Use cached schema reading for PostgreSQL 11+ to minimize database calls
        Tables cachedTables =
                PostgresSchemaCache.getInstance()
                        .getOrLoadTables(
                                jdbcConnection,
                                dbzConfig.databaseName(),
                                null, // Read all schemas
                                dbzConfig.getTableFilters().dataCollectionFilter(),
                                false // Don't force refresh
                                );

        // Copy relevant tables to the provided Tables object
        for (TableId tableId : targetTableIds) {
            Table table = cachedTables.forTable(tableId);
            if (table != null) {
                tables.overwriteTable(table);
            }
        }
    }

    @Override
    protected Table handleMissingTable(TableId tableId, Tables tables) throws SQLException {
        // For PostgreSQL 11+, just use individual table read
        return readIndividualTable(tableId);
    }

    @Override
    protected Table enhanceTableForVersion(Table table, TableId tableId) throws SQLException {
        // PostgreSQL 11+ doesn't need special enhancements like PG10
        return table;
    }

    @Override
    protected SchemaChangeEvent createSchemaChangeEvent(
            PostgresPartition partition,
            PostgresOffsetContext offsetContext,
            Table table,
            TableId tableId) {

        // Use basic schema change event for PostgreSQL 11+
        return createBasicSchemaChangeEvent(partition, offsetContext, table, tableId);
    }
}
