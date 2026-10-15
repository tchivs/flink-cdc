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

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CustomPostgresSchema}. */
class CustomPostgresSchemaTest extends PostgresTestBase {

    private static final String SCHEMA_NAME = "similar_names";

    private final UniqueDatabase database =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private RecordingPostgresConnection connection;

    @BeforeEach
    void before() {
        database.createAndInitialize();
    }

    @AfterEach
    void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void singleTableLookupReadsOnlyTheRequestedTable() {
        PostgresSourceConfig config = sourceConfig(".*");
        List<TableId> captured = new PostgresDialect(config).discoverDataCollections(config);
        // '_' is a LIKE wildcard, so ndi_pg_user_sink_1 also pattern-matches ndi_pg_userbsink_1
        TableId underscore = find(captured, "ndi_pg_user_sink_1");
        TableId lookalike = find(captured, "ndi_pg_userbsink_1");
        CustomPostgresSchema schema = new CustomPostgresSchema(connection(config), config);

        TableChange change = schema.getTableSchema(underscore);

        assertThat(connection.reads).containsExactly(Set.of(underscore));
        assertThat(change.getTable().id()).isEqualTo(underscore);
        assertThat(change.getTable().retrieveColumnNames())
                .containsExactly("id", "name", "address");

        schema.getTableSchema(lookalike);
        assertThat(connection.reads.get(1)).containsExactly(lookalike);
    }

    @Test
    void singleTableLookupsMatchTheBatchLookup() {
        PostgresSourceConfig config = sourceConfig(".*");
        List<TableId> captured = new PostgresDialect(config).discoverDataCollections(config);
        assertThat(captured).hasSize(4);

        Map<TableId, TableChange> batch =
                new CustomPostgresSchema(connection(config), config).getTableSchema(captured);
        assertThat(connection.reads).containsExactly(new HashSet<>(captured));

        for (TableId tableId : captured) {
            TableChange single =
                    new CustomPostgresSchema(connection, config).getTableSchema(tableId);
            assertThat(single.getTable()).isEqualTo(batch.get(tableId).getTable());
        }
    }

    @Test
    void tableOutsideTheConnectorFilterStillFails() {
        PostgresSourceConfig allTables = sourceConfig(".*");
        TableId excluded =
                find(
                        new PostgresDialect(allTables).discoverDataCollections(allTables),
                        "ndi_pg_userbsink_1");
        PostgresSourceConfig config = sourceConfig("ndi_pg_user_sink_1");
        CustomPostgresSchema schema = new CustomPostgresSchema(connection(config), config);

        assertThatThrownBy(() -> schema.getTableSchema(excluded))
                .isInstanceOf(NullPointerException.class);
    }

    private PostgresSourceConfig sourceConfig(String tableRegex) {
        return getMockPostgresSourceConfigFactory(database, SCHEMA_NAME, tableRegex, 10).create(0);
    }

    private RecordingPostgresConnection connection(PostgresSourceConfig config) {
        connection = new RecordingPostgresConnection(config.getDbzConnectorConfig());
        return connection;
    }

    private static TableId find(List<TableId> tableIds, String table) {
        Optional<TableId> match =
                tableIds.stream().filter(tableId -> table.equals(tableId.table())).findFirst();
        assertThat(match).as("captured table %s", table).isPresent();
        return match.get();
    }

    /** Records which tables each {@code readSchema} call materialized. */
    private static final class RecordingPostgresConnection extends PostgresConnection {

        private final List<Set<TableId>> reads = new ArrayList<>();

        private RecordingPostgresConnection(PostgresConnectorConfig config) {
            super(
                    config.getJdbcConfig(),
                    newPostgresValueConverterBuilder(config),
                    "custom-postgres-schema-test");
        }

        @Override
        public void readSchema(
                Tables tables,
                String databaseCatalog,
                String schemaNamePattern,
                Tables.TableFilter tableFilter,
                Tables.ColumnNameFilter columnFilter,
                boolean removeTablesNotFoundInJdbc)
                throws SQLException {
            super.readSchema(
                    tables,
                    databaseCatalog,
                    schemaNamePattern,
                    tableFilter,
                    columnFilter,
                    removeTablesNotFoundInJdbc);
            reads.add(new HashSet<>(tables.tableIds()));
        }
    }
}
