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

package org.apache.flink.cdc.connectors.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** End-to-end tests for postgres-cdc PG10 partitioned table routing. */
@Testcontainers
class PostgresPartitionedPg10E2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresPartitionedPg10E2eITCase.class);
    private static final String PG_TEST_USER = "postgres";
    private static final String PG_TEST_PASSWORD = "postgres";
    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String INTER_CONTAINER_PG_ALIAS = "postgres-pg10";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String MYSQL_SINK_TABLE = "products_partitioned_pg10_sink";

    private static final DockerImageName PG_IMAGE =
            DockerImageName.parse("postgres:10").asCompatibleSubstituteFor("postgres");

    private static final Path postgresCdcJar = TestUtils.getResource("postgres-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    private static final String FLINK_PROPERTIES =
            String.join(
                    "\n",
                    Arrays.asList(
                            "jobmanager.rpc.address: jobmanager",
                            "taskmanager.numberOfTaskSlots: 10",
                            "parallelism.default: 1",
                            "execution.checkpointing.interval: 10000"));

    @Container
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername(PG_TEST_USER)
                    .withPassword(PG_TEST_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_PG_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            "fsync=off",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_wal_senders=20",
                            "-c",
                            "max_replication_slots=20")
                    .withReuse(true);

    @BeforeEach
    public void before() {
        super.before();
        initializePostgresTable("inventory_partitioned_pg10");
        overrideFlinkProperties(FLINK_PROPERTIES);
    }

    @AfterEach
    public void after() {
        super.after();
    }

    @Test
    void testPostgresPartitionedPg10ChildEventsRoutedAsParentTable() throws Exception {
        String publicationName = "my_pub";
        String slotName = getSlotName("flink_pg10_partitioned_");

        createPublicationAndSlot(publicationName, slotName);
        String mysqlUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        initializeMySqlSinkTable(mysqlUrl);

        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute("ANALYZE;");
        }

        List<String> sourceSql =
                Arrays.asList(
                        "CREATE TABLE products_source (",
                        " schema_name STRING METADATA VIRTUAL,",
                        " table_name STRING METADATA VIRTUAL,",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " country STRING,",
                        " primary key (`id`, `country`) not enforced",
                        ") WITH (",
                        " 'connector' = 'postgres-cdc',",
                        " 'hostname' = '" + INTER_CONTAINER_PG_ALIAS + "',",
                        " 'port' = '" + POSTGRESQL_PORT + "',",
                        " 'username' = '" + PG_TEST_USER + "',",
                        " 'password' = '" + PG_TEST_PASSWORD + "',",
                        " 'database-name' = '" + POSTGRES.getDatabaseName() + "',",
                        " 'schema-name' = 'inventory_partitioned',",
                        " 'table-name' = 'products',",
                        " 'scan.include-partitioned-tables.enabled' = 'true',",
                        " 'decoding.plugin.name' = 'pgoutput',",
                        " 'debezium.publication.name' = '" + publicationName + "',",
                        " 'slot.name' = '" + slotName + "',",
                        " 'debezium.slot.drop.on.stop' = 'true',",
                        " 'scan.startup.mode' = 'initial'",
                        ");");

        List<String> sinkSql =
                Arrays.asList(
                        "CREATE TABLE products_sink (",
                        " schema_name STRING,",
                        " table_name STRING,",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " country STRING,",
                        " primary key (`id`, `country`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = '" + MYSQL_SINK_TABLE + "',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT schema_name, table_name, id, name, description, weight, country FROM products_source;");

        List<String> sqlLines =
                Stream.concat(sourceSql.stream(), sinkSql.stream()).collect(Collectors.toList());

        submitSQLJob(sqlLines, postgresCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        List<String> expectedSnapshotRows =
                Arrays.asList(
                        "inventory_partitioned,products,101,scooter,Small 2-wheel scooter,3.14,us",
                        "inventory_partitioned,products,102,car battery,12V car battery,8.1,us",
                        "inventory_partitioned,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,us",
                        "inventory_partitioned,products,104,hammer,12oz carpenter's hammer,0.75,us",
                        "inventory_partitioned,products,105,hammer,14oz carpenter's hammer,0.875,us",
                        "inventory_partitioned,products,106,hammer,16oz carpenter's hammer,1.0,uk",
                        "inventory_partitioned,products,107,rocks,box of assorted rocks,5.3,uk",
                        "inventory_partitioned,products,108,jacket,water resistent black wind breaker,0.1,uk",
                        "inventory_partitioned,products,109,spare tire,24 inch spare tire,22.2,uk");
        assertSinkRowsWithTimeout(mysqlUrl, expectedSnapshotRows, Duration.ofMinutes(2));
        Thread.sleep(5000L);

        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_uk (name, description, weight, country) VALUES ('helmet','bike helmet',0.5,'uk');");
            statement.execute(
                    "INSERT INTO inventory_partitioned.products_us (name, description, weight, country) VALUES ('gloves','winter gloves',0.2,'us');");
        } catch (SQLException e) {
            LOG.error("Update partitioned table for CDC failed.", e);
            throw e;
        }

        List<String> expectedRows =
                Arrays.asList(
                        "inventory_partitioned,products,101,scooter,Small 2-wheel scooter,3.14,us",
                        "inventory_partitioned,products,102,car battery,12V car battery,8.1,us",
                        "inventory_partitioned,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,us",
                        "inventory_partitioned,products,104,hammer,12oz carpenter's hammer,0.75,us",
                        "inventory_partitioned,products,105,hammer,14oz carpenter's hammer,0.875,us",
                        "inventory_partitioned,products,106,hammer,16oz carpenter's hammer,1.0,uk",
                        "inventory_partitioned,products,107,rocks,box of assorted rocks,5.3,uk",
                        "inventory_partitioned,products,108,jacket,water resistent black wind breaker,0.1,uk",
                        "inventory_partitioned,products,109,spare tire,24 inch spare tire,22.2,uk",
                        "inventory_partitioned,products,110,helmet,bike helmet,0.5,uk",
                        "inventory_partitioned,products,111,gloves,winter gloves,0.2,us");

        assertSinkRowsWithTimeout(mysqlUrl, expectedRows, Duration.ofMinutes(2));
    }

    public static String getSlotName(String prefix) {
        final Random random = new Random();
        int id = random.nextInt(9000);
        return prefix + id;
    }

    private void createPublicationAndSlot(String publicationName, String slotName) {
        try (Connection conn = getPgJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(String.format("DROP PUBLICATION IF EXISTS %s", publicationName));
            try {
                statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", slotName));
            } catch (SQLException ignored) {
                // ignore missing slot
            }
            statement.execute(
                    String.format(
                            "CREATE PUBLICATION %s FOR TABLE inventory_partitioned.products_uk, inventory_partitioned.products_us",
                            publicationName));
            statement.execute(
                    String.format(
                            "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                            slotName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializePostgresTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile =
                PostgresPartitionedPg10E2eITCase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try {
            Class.forName(PG_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (Connection connection = getPgJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private void assertSinkRowsWithTimeout(
            String mysqlUrl, List<String> expectedRows, Duration timeout)
            throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        List<String> actualRows = new ArrayList<>();
        List<String> sortedExpected = new ArrayList<>(expectedRows);
        sortedExpected.sort(String::compareTo);

        while (System.currentTimeMillis() < deadline) {
            actualRows = fetchSinkRows(mysqlUrl);
            if (actualRows.equals(sortedExpected)) {
                return;
            }
            Thread.sleep(1000L);
        }

        Assertions.assertThat(actualRows).isEqualTo(sortedExpected);
    }

    private List<String> fetchSinkRows(String mysqlUrl) throws SQLException {
        List<String> rows = new ArrayList<>();
        String query =
                "SELECT schema_name, table_name, id, name, description, weight, country FROM "
                        + MYSQL_SINK_TABLE;
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                PreparedStatement statement = conn.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                rows.add(
                        String.join(
                                ",",
                                resultSet.getString("schema_name"),
                                resultSet.getString("table_name"),
                                String.valueOf(resultSet.getInt("id")),
                                resultSet.getString("name"),
                                resultSet.getString("description"),
                                String.valueOf(resultSet.getDouble("weight")),
                                resultSet.getString("country")));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private void initializeMySqlSinkTable(String mysqlUrl) {
        String ddl =
                "CREATE TABLE IF NOT EXISTS "
                        + MYSQL_SINK_TABLE
                        + " ("
                        + "schema_name VARCHAR(255),"
                        + "table_name VARCHAR(255),"
                        + "id INT NOT NULL,"
                        + "name VARCHAR(255),"
                        + "description VARCHAR(512),"
                        + "weight DOUBLE,"
                        + "country VARCHAR(20),"
                        + "PRIMARY KEY (id, country)"
                        + ")";
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + MYSQL_SINK_TABLE);
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
