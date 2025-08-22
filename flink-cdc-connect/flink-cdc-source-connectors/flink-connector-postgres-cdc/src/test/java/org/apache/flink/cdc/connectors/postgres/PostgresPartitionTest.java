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

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests PostgreSQL partition table handling across multiple versions. */
@PostgresVersion(
        versions = {"10", "11", "12", "13", "14", "15", "16"},
        schema = PostgresPartitionTest.PARTITION)
class PostgresPartitionTest extends PostgresVersionedTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionTest.class);
    public static final String PARTITION = "partition";

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(2)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    @TestTemplate
    void testPartitionTablePrimaryKeyInheritance() throws Exception {
        LOG.info("Testing partition table primary key handling");

        try (Connection conn = getJdbcConnectionWithSearchPath();
                Statement stmt = conn.createStatement()) {

            // Verify the partition table exists and has data
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders_2023_q1");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isGreaterThan(0);

            // Verify primary key constraint exists on the partition table
            ResultSet pkRs =
                    stmt.executeQuery(
                            "SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'orders_2023_q1' AND constraint_type = 'PRIMARY KEY' "
                                    + "AND table_schema = 'partition'");
            assertThat(pkRs.next()).isTrue();
            assertThat(pkRs.getString("constraint_name")).isNotNull();

            // Verify the primary key columns
            ResultSet colRs =
                    stmt.executeQuery(
                            "SELECT column_name FROM information_schema.key_column_usage "
                                    + "WHERE table_name = 'orders_2023_q1' AND table_schema = 'partition' "
                                    + "AND constraint_name IN "
                                    + "(SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'orders_2023_q1' AND table_schema = 'partition' "
                                    + "AND constraint_type = 'PRIMARY KEY') "
                                    + "ORDER BY ordinal_position");

            assertThat(colRs.next()).isTrue();
            assertThat(colRs.getString("column_name")).isEqualTo("order_id");
            assertThat(colRs.next()).isTrue();
            assertThat(colRs.getString("column_name")).isEqualTo("order_date");

            LOG.info("✓ Verified partition table with primary key");
        }

        // Test cross-partition queries
        try (Connection conn = getJdbcConnectionWithSearchPath();
                Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders_2023_q2");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isGreaterThan(0);

            ResultSet parentRs = stmt.executeQuery("SELECT COUNT(*) FROM orders");
            assertThat(parentRs.next()).isTrue();
            assertThat(parentRs.getInt(1)).isGreaterThan(0);

            LOG.info("✓ Verified cross-partition queries");
        }
    }

    @TestTemplate
    void testListPartitionTableWithPrimaryKey() throws Exception {
        LOG.info("Testing list partition table primary key handling");

        // Test the list partitioned table from partition.sql
        try (Connection conn = getJdbcConnectionWithSearchPath();
                Statement stmt = conn.createStatement()) {

            // Verify the list partition tables exist and have data
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM products_electronics");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isGreaterThan(0);

            // Verify primary key constraint exists on the list partition table
            ResultSet pkRs =
                    stmt.executeQuery(
                            "SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'products_electronics' AND constraint_type = 'PRIMARY KEY' "
                                    + "AND table_schema = 'partition'");
            assertThat(pkRs.next()).isTrue();
            assertThat(pkRs.getString("constraint_name")).isNotNull();

            // Verify parent table query across partitions
            ResultSet parentRs = stmt.executeQuery("SELECT COUNT(*) FROM products_by_category");
            assertThat(parentRs.next()).isTrue();
            assertThat(parentRs.getInt(1)).isGreaterThan(0);

            LOG.info("✓ Verified list partition table");
        }
    }

    @TestTemplate
    void testHashPartitionTableSupport() throws Exception {
        String currentVersion = getPostgresVersion();
        LOG.info("Testing hash partition support");

        if (isPostgresVersionAtLeast(11)) {
            try (Connection conn = getJdbcConnectionWithSearchPath();
                    Statement stmt = conn.createStatement()) {

                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT COUNT(*) FROM information_schema.tables "
                                        + "WHERE table_schema = 'partition' AND table_name LIKE 'user_sessions_%'");

                if (rs.next() && rs.getInt(1) > 0) {
                    ResultSet dataRs = stmt.executeQuery("SELECT COUNT(*) FROM user_sessions");
                    assertThat(dataRs.next()).isTrue();
                    LOG.info("✓ Hash partitioning supported on PG {}", currentVersion);
                } else {
                    LOG.info("◦ Hash partitioning tables not found for PG {}", currentVersion);
                }
            }
        } else {
            LOG.info("◦ Hash partitioning requires PG 11+, current: {}", currentVersion);
        }
    }

    @TestTemplate
    void testNonPartitionTableNotAffected() throws Exception {
        LOG.info("Testing non-partition tables");

        try (Connection conn = getJdbcConnectionWithSearchPath();
                Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM customers");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isGreaterThan(0);

            ResultSet pkRs =
                    stmt.executeQuery(
                            "SELECT constraint_name FROM information_schema.table_constraints "
                                    + "WHERE table_name = 'customers' AND constraint_type = 'PRIMARY KEY' "
                                    + "AND table_schema = 'partition'");
            assertThat(pkRs.next()).isTrue();
            assertThat(pkRs.getString("constraint_name")).isNotNull();

            LOG.info("✓ Verified regular table");
        }
    }

    @TestTemplate
    void testPartitionRoutingEndToEnd() throws Exception {
        LOG.info("Testing end-to-end partition routing from WAL events to parent tables");

        // Build Debezium properties for partition routing
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("partition.optimize", "true");
        debeziumProps.setProperty(
                "partition.origin", "partition\\.orders_.*|partition\\.products_.*");
        debeziumProps.setProperty(
                "partition.target", "partition.orders|partition.products_by_category");

        // Build a Postgres CDC source that only lists parent tables
        PostgresSourceBuilder.PostgresIncrementalSource<String> source =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(getContainer().getHost())
                        .port(getContainer().getMappedPort(5432))
                        .database(getTestDatabase().getDatabaseName())
                        .schemaList(PARTITION)
                        .tableList("partition.orders", "partition.products_by_category")
                        .username(getContainer().getUsername())
                        .password(getContainer().getPassword())
                        .slotName(getTestSlotName())
                        .decodingPluginName("pgoutput")
                        .deserializer(
                                new org.apache.flink.cdc.debezium
                                        .JsonDebeziumDeserializationSchema())
                        .splitSize(4)
                        .debeziumProperties(debeziumProps)
                        .build();

        // Start a tiny Flink job to collect records
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        var it =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "PostgresPartitionSource")
                        .setParallelism(1)
                        .executeAndCollect();

        // Drain some snapshot records first to ensure the pipeline is running
        List<String> snapshot = new ArrayList<>();
        for (int i = 0; i < 5 && it.hasNext(); i++) {
            snapshot.add(it.next());
        }
        LOG.info("Drained {} snapshot records", snapshot.size());

        // Produce WAL events on child partitions
        try (Connection conn = getJdbcConnectionWithSearchPath();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "INSERT INTO orders_2023_q1(order_id, customer_id, order_date, total_amount, status) "
                            + "VALUES (3001, 9, '2023-02-01', 123.45, 'completed')");
            stmt.executeUpdate(
                    "INSERT INTO products_electronics(product_id, product_name, category, price, in_stock) "
                            + "VALUES (4001, 'Router', 'electronics', 129.99, true)");
        }

        // Collect several records and assert they are routed to parent tables in the source
        // metadata
        boolean sawOrdersOnParent = false;
        boolean sawProductsOnParent = false;
        long deadline = System.currentTimeMillis() + 30_000; // 30s timeout
        while (System.currentTimeMillis() < deadline
                && !(sawOrdersOnParent && sawProductsOnParent)) {
            if (!it.hasNext()) {
                Thread.sleep(200);
                continue;
            }
            String rec = it.next();
            if (rec.contains("\"schema\":\"partition\"") && rec.contains("\"table\":\"orders\"")) {
                sawOrdersOnParent = true;
            }
            if (rec.contains("\"schema\":\"partition\"")
                    && rec.contains("\"table\":\"products_by_category\"")) {
                sawProductsOnParent = true;
            }
        }

        assertThat(sawOrdersOnParent)
                .withFailMessage("Did not observe routed event for orders parent table")
                .isTrue();
        assertThat(sawProductsOnParent)
                .withFailMessage(
                        "Did not observe routed event for products_by_category parent table")
                .isTrue();

        // close iterator to stop the job
        it.close();
    }
}
