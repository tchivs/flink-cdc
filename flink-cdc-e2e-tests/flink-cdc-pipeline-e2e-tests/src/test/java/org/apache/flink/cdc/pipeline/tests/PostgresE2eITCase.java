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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;

import static org.apache.flink.cdc.connectors.postgres.PostgresTestBase.getJdbcConnection;
import static org.apache.flink.cdc.connectors.postgres.PostgresTestBase.getSlotName;

/** End-to-end tests for postgres cdc pipeline job. */
public class PostgresE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // Postgres Variables (we always use Postgres as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String POSTGRES_TEST_USER = "postgres";
    protected static final String POSTGRES_TEST_PASSWORD = "postgres";
    protected static final String INTER_CONTAINER_POSTGRES_ALIAS = "postgres";

    // use official postgresql image to support pgoutput plugin
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("postgres:14").asCompatibleSubstituteFor("postgres");

    @Container
    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetworkAliases(INTER_CONTAINER_POSTGRES_ALIAS)
                    .withNetwork(NETWORK)
                    .withUsername(POSTGRES_TEST_USER)
                    .withPassword(POSTGRES_TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical")
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(30L));

    private final UniqueDatabase postgresInventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "postgres_inventory",
                    POSTGRES_TEST_USER,
                    POSTGRES_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter = (s) -> String.format(s, "inventory");
    private String slotName;

    @BeforeEach
    public void before() throws Exception {
        super.before();
        slotName = getSlotName();
        postgresInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        postgresInventoryDatabase.removeSlot(slotName);
        postgresInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.inventory.\\.*\n"
                                + "  slot.name: %s\n"
                                + "  scan.startup.mode: initial\n"
                                + "  server-time-zone: UTC\n"
                                + "  connect.timeout: 120s\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_POSTGRES_ALIAS,
                        5432,
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD,
                        postgresInventoryDatabase.getDatabaseName(),
                        slotName,
                        parallelism);
        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL 'nextval('inventory.customers_id_seq'::regclass)',`first_name` VARCHAR(255) NOT NULL,`last_name` VARCHAR(255) NOT NULL,`email` VARCHAR(255) NOT NULL}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, Anne, Kretchmar, annek@noanswer.org], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, Edward, Walker, ed@walker.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, George, Bailey, gbailey@foobar.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, Sally, Thomas, sally.thomas@acme.com], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL 'nextval('inventory.products_id_seq'::regclass)',`name` VARCHAR(255) NOT NULL,`description` VARCHAR(512),`weight` FLOAT}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, postgresInventoryDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");

            // Perform DML changes after the wal log is generated
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.products, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.products, before=[107, rocks, box of assorted rocks, 5.3], after=[107, rocks, box of assorted rocks, 5.1], op=UPDATE, meta=()}");
        } catch (Exception e) {
            LOG.error("Update table for CDC failed.", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPartitionTableParentConfiguration() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.inventory.orders\n" // Parent partition table
                                + "  slot.name: %s\n"
                                + "  scan.startup.mode: initial\n"
                                + "  server-time-zone: UTC\n"
                                + "  connect.timeout: 120s\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_POSTGRES_ALIAS,
                        5432,
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD,
                        postgresInventoryDatabase.getDatabaseName(),
                        slotName,
                        parallelism);

        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar);

        waitUntilJobRunning(Duration.ofSeconds(30));
        // Verify snapshot data from all partitions
        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=inventory.orders, schema=columns={`id` INT NOT NULL,`customer_id` INT NOT NULL,`order_date` DATE NOT NULL,`product_id` INT NOT NULL,`quantity` INT NOT NULL,`total_amount` DECIMAL(10, 2) NOT NULL,`status` STRING}, primaryKeys=id,order_date, options=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[1, 101, 2023-11-15, 101, 2, 199.99, COMPLETED], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[2, 102, 2023-12-20, 102, 1, 89.50, SHIPPED], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[3, 103, 2024-01-10, 103, 3, 150.75, PENDING], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[4, 101, 2024-02-14, 101, 1, 99.99, COMPLETED], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[5, 104, 2024-03-18, 104, 2, 299.98, PROCESSING], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.orders, before=[], after=[6, 102, 2024-05-22, 105, 1, 45.99, SHIPPED], op=INSERT, meta=()}");

        LOG.info("Begin partition table incremental reading stage.");

        // Test streaming changes to partition table
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, postgresInventoryDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {

            // Insert into different partitions
            stat.execute(
                    "INSERT INTO inventory.orders (customer_id, order_date, product_id, quantity, total_amount, status) "
                            + "VALUES (105, '2024-01-25', 106, 2, 149.99, 'PENDING')");
            stat.execute(
                    "INSERT INTO inventory.orders (customer_id, order_date, product_id, quantity, total_amount, status) "
                            + "VALUES (106, '2024-04-15', 107, 3, 299.97, 'COMPLETED')");

            // Update existing records
            stat.execute("UPDATE inventory.orders SET status='COMPLETED' WHERE id=3");
            stat.execute("UPDATE inventory.orders SET quantity=5, total_amount=499.95 WHERE id=5");

            // Verify streaming changes
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.orders, before=[], after=[7, 105, 2024-01-25, 106, 2, 149.99, PENDING], op=INSERT, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.orders, before=[], after=[8, 106, 2024-04-15, 107, 3, 299.97, COMPLETED], op=INSERT, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.orders, before=[3, 103, 2024-01-10, 103, 3, 150.75, PENDING], after=[3, 103, 2024-01-10, 103, 3, 150.75, COMPLETED], op=UPDATE, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.orders, before=[5, 104, 2024-03-18, 104, 2, 299.98, PROCESSING], after=[5, 104, 2024-03-18, 104, 5, 499.95, PROCESSING], op=UPDATE, meta=()}");

        } catch (Exception e) {
            LOG.error("Update partition table for CDC failed.", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    void testMultiplePartitionTablesConfiguration() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.inventory.(orders|inventory|user_sessions)\n" // Multiple partition tables
                                + "  slot.name: %s\n"
                                + "  scan.startup.mode: initial\n"
                                + "  server-time-zone: UTC\n"
                                + "  connect.timeout: 120s\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_POSTGRES_ALIAS,
                        5432,
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD,
                        postgresInventoryDatabase.getDatabaseName(),
                        slotName,
                        parallelism);

        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar);

        // Wait for create table events for all partition tables
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=inventory.orders, schema=columns={`id` INT NOT NULL,`customer_id` INT NOT NULL,`order_date` DATE NOT NULL,`product_id` INT NOT NULL,`quantity` INT NOT NULL,`total_amount` DECIMAL(10, 2) NOT NULL,`status` STRING}, primaryKeys=id,order_date, options=()}",
                "CreateTableEvent{tableId=inventory.inventory, schema=columns={`id` INT NOT NULL,`product_id` INT NOT NULL,`warehouse_location` STRING NOT NULL,`region` STRING NOT NULL,`stock_quantity` INT NOT NULL,`last_updated` TIMESTAMP_LTZ(6)}, primaryKeys=id,region, options=()}",
                "CreateTableEvent{tableId=inventory.user_sessions, schema=columns={`id` INT NOT NULL,`user_id` BIGINT NOT NULL,`session_start` TIMESTAMP_LTZ(6),`session_end` TIMESTAMP_LTZ(6),`page_views` INT,`duration_minutes` INT}, primaryKeys=id,user_id, options=()}");
        LOG.info("Multiple partition tables initialized successfully.");
        // Test streaming changes across multiple partition tables
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, postgresInventoryDatabase.getDatabaseName());
                Statement stat = conn.createStatement()) {

            // Changes to orders partition table
            stat.execute(
                    "INSERT INTO inventory.orders (customer_id, order_date, product_id, quantity, total_amount, status) "
                            + "VALUES (110, '2024-06-01', 108, 1, 79.99, 'PENDING')");

            // Changes to inventory partition table
            stat.execute(
                    "INSERT INTO inventory.inventory (product_id, warehouse_location, region, stock_quantity) "
                            + "VALUES (108, 'Phoenix-WH', 'southwest', 200)");

            // Changes to user_sessions partition table
            stat.execute(
                    "INSERT INTO inventory.user_sessions (user_id, session_start, page_views, duration_minutes) "
                            + "VALUES (1006, '2024-06-01 09:00:00', 12, 20)");

            // Verify at least one event from each partition table
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.orders, before=[], after=[7, 110, 2024-06-01, 108, 1, 79.99, PENDING], op=INSERT, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.inventory, before=[], after=[7, 108, Phoenix-WH, southwest, 200");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=inventory.user_sessions, before=[], after=[6, 1006");

        } catch (Exception e) {
            LOG.error("Update multiple partition tables for CDC failed.", e);
            throw new RuntimeException(e);
        }
    }
}
