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
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.postgres.PostgresPg10TestBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** PG10 Testcontainers IT for partitioned parent snapshot split generation. */
class PostgresPg10PartitionSnapshotSplitITCase extends PostgresPg10TestBase {

    private static final int DEFAULT_PARALLELISM = 4;

    @Test
    void testSnapshotSplitUsesChildPrimaryKeyForPg10PartitionParent() throws Exception {
        UniqueDatabase inventoryPartitionedDatabase = createInventoryPartitionedDatabase();
        createDatabase(inventoryPartitionedDatabase.getDatabaseName());
        initializePg10PartitionedTableWithChildPrimaryKeys(
                inventoryPartitionedDatabase.getDatabaseName());

        PostgresSourceConfigFactory configFactory =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 3);
        configFactory.setIncludePartitionedTables(true);

        PostgresSourceConfig config = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(config);
        TableId parentTable = new TableId(null, "inventory_partitioned", "products");

        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(config, dialect);

        assertThat(snapshotSplits).isNotEmpty();
        assertThat(snapshotSplits)
                .allSatisfy(split -> assertThat(split.getTableId()).isEqualTo(parentTable));
        assertThat(snapshotSplits.get(0).getTableSchemas())
                .containsKey(parentTable)
                .satisfies(
                        tableSchemas ->
                                assertThat(
                                                tableSchemas
                                                        .get(parentTable)
                                                        .getTable()
                                                        .primaryKeyColumnNames())
                                        .containsExactly("id"));
    }

    private List<SnapshotSplit> getSnapshotSplits(
            PostgresSourceConfig sourceConfig, PostgresDialect sourceDialect) throws Exception {
        List<TableId> discoverTables = sourceDialect.discoverDataCollections(sourceConfig);
        OffsetFactory offsetFactory = new PostgresOffsetFactory();
        SnapshotSplitAssigner<JdbcSourceConfig> snapshotSplitAssigner =
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        DEFAULT_PARALLELISM,
                        discoverTables,
                        sourceDialect.isDataCollectionIdCaseSensitive(sourceConfig),
                        sourceDialect,
                        offsetFactory);
        snapshotSplitAssigner.initEnumeratorMetrics(
                new SourceEnumeratorMetrics(
                        UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup()));
        snapshotSplitAssigner.open();

        try {
            List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
            Optional<SourceSplitBase> split = snapshotSplitAssigner.getNext();
            while (split.isPresent()) {
                snapshotSplitList.add(split.get().asSnapshotSplit());
                split = snapshotSplitAssigner.getNext();
            }
            return snapshotSplitList;
        } finally {
            snapshotSplitAssigner.close();
        }
    }

    private UniqueDatabase createInventoryPartitionedDatabase() {
        return new UniqueDatabase(
                POSTGRES_CONTAINER,
                "postgres_pg10_snapshot_split",
                "inventory_partitioned",
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }

    private void createDatabase(String databaseName) throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
    }

    private void initializePg10PartitionedTableWithChildPrimaryKeys(String databaseName)
            throws Exception {
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS inventory_partitioned CASCADE");
            statement.execute("CREATE SCHEMA inventory_partitioned");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products ("
                            + " id SERIAL NOT NULL,"
                            + " name VARCHAR(255) NOT NULL DEFAULT 'flink',"
                            + " description VARCHAR(512),"
                            + " weight FLOAT,"
                            + " country VARCHAR(20) NOT NULL"
                            + ") PARTITION BY LIST(country)");
            statement.execute(
                    "ALTER SEQUENCE inventory_partitioned.products_id_seq RESTART WITH 101");
            statement.execute("ALTER TABLE inventory_partitioned.products REPLICA IDENTITY FULL");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_uk PARTITION OF inventory_partitioned.products FOR VALUES IN ('uk')");
            statement.execute(
                    "CREATE TABLE inventory_partitioned.products_us PARTITION OF inventory_partitioned.products FOR VALUES IN ('us')");
            statement.execute("ALTER TABLE inventory_partitioned.products_uk ADD PRIMARY KEY (id)");
            statement.execute("ALTER TABLE inventory_partitioned.products_us ADD PRIMARY KEY (id)");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products VALUES "
                            + "(default,'scooter','Small 2-wheel scooter',3.14, 'us'),"
                            + "(default,'car battery','12V car battery',8.1, 'us'),"
                            + "(default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8, 'us'),"
                            + "(default,'hammer','12oz carpenter''s hammer',0.75, 'us'),"
                            + "(default,'hammer','14oz carpenter''s hammer',0.875, 'us'),"
                            + "(default,'hammer','16oz carpenter''s hammer',1.0, 'uk'),"
                            + "(default,'rocks','box of assorted rocks',5.3, 'uk'),"
                            + "(default,'jacket','water resistent black wind breaker',0.1, 'uk'),"
                            + "(default,'spare tire','24 inch spare tire',22.2, 'uk')");
            statement.execute("ANALYZE inventory_partitioned.products");
        }
    }
}
