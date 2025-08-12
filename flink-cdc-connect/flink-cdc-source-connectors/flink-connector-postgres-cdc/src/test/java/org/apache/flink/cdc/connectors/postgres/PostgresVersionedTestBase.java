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

import org.apache.flink.cdc.connectors.postgres.source.PostgresConnectionPoolFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersionExtension;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

/**
 * Abstract base class for PostgreSQL CDC testing with multi-version support.
 *
 * <p>This class provides a versioned testing framework that supports:
 *
 * <ul>
 *   <li>Multiple PostgreSQL versions through {@link
 *       org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion} annotations
 *   <li>Non-static container instances for better test isolation
 *   <li>Automatic container lifecycle management
 *   <li>Backward compatibility with existing test utilities
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Single version testing
 * @PostgresVersion(version = "14")
 * class MyTest extends PostgresVersionedTestBase {
 *     @TestTemplate
 *     void testSomething() {
 *         // Test implementation using getContainer()
 *     }
 * }
 *
 * // Multi-version testing
 * @PostgresVersion(version = "14")
 * @PostgresVersion(version = "15")
 * @PostgresVersion(version = "16")
 * class MyMultiVersionTest extends PostgresVersionedTestBase {
 *     @TestTemplate
 *     void testAcrossVersions() {
 *         // This test will run on PostgreSQL 14, 15, and 16
 *     }
 * }
 * }</pre>
 */
@ExtendWith(PostgresVersionExtension.class)
public abstract class PostgresVersionedTestBase extends AbstractTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(PostgresVersionedTestBase.class);
    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    public static final String DEFAULT_DB = "postgres";
    public static final String TEST_USER = "postgres";
    public static final String TEST_PASSWORD = "postgres";

    // Instance variables for per-test container management
    protected PostgreSQLContainer<?> currentContainer;
    protected Network currentNetwork;

    /**
     * Sets the current container and network for this test instance. This method is called
     * automatically by the PostgresVersionExtension.
     *
     * @param container the PostgreSQL container for this test
     * @param network the network for this test
     */
    public void setContainerAndNetwork(PostgreSQLContainer<?> container, Network network) {
        this.currentContainer = container;
        this.currentNetwork = network;
    }

    /**
     * Gets the current PostgreSQL container for this test.
     *
     * @return the current PostgreSQL container
     * @throws IllegalStateException if no container is available
     */
    protected PostgreSQLContainer<?> getContainer() {
        if (currentContainer == null) {
            throw new IllegalStateException(
                    "No PostgreSQL container available. "
                            + "Make sure your test method is annotated with @TestTemplate and "
                            + "your test class extends PostgresVersionedTestBase.");
        }
        return currentContainer;
    }

    /**
     * Gets the current network for this test.
     *
     * @return the current network
     * @throws IllegalStateException if no network is available
     */
    protected Network getNetwork() {
        if (currentNetwork == null) {
            throw new IllegalStateException(
                    "No network available. "
                            + "Make sure your test method is annotated with @TestTemplate and "
                            + "your test class extends PostgresVersionedTestBase.");
        }
        return currentNetwork;
    }

    /**
     * Gets a JDBC connection to the current PostgreSQL container.
     *
     * @return a JDBC connection
     * @throws SQLException if connection fails
     */
    protected Connection getJdbcConnection() throws SQLException {
        return getJdbcConnection(getContainer());
    }

    /**
     * Gets a JDBC connection to the specified PostgreSQL container.
     *
     * @param container the PostgreSQL container
     * @return a JDBC connection
     * @throws SQLException if connection fails
     */
    protected Connection getJdbcConnection(PostgreSQLContainer<?> container) throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    /**
     * Gets a JDBC connection to a specific database in the current container.
     *
     * @param databaseName the database name
     * @return a JDBC connection
     * @throws SQLException if connection fails
     */
    protected Connection getJdbcConnection(String databaseName) throws SQLException {
        return getJdbcConnection(getContainer(), databaseName);
    }

    /**
     * Gets a JDBC connection to a specific database in the specified container.
     *
     * @param container the PostgreSQL container
     * @param databaseName the database name
     * @return a JDBC connection
     * @throws SQLException if connection fails
     */
    public static Connection getJdbcConnection(
            PostgreSQLContainer<?> container, String databaseName) throws SQLException {
        String jdbcUrl =
                String.format(
                        PostgresConnectionPoolFactory.JDBC_URL_PATTERN,
                        container.getHost(),
                        container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        databaseName);
        return DriverManager.getConnection(
                jdbcUrl, container.getUsername(), container.getPassword());
    }

    /**
     * Gets a PostgresConnection for the specified database configuration.
     *
     * @param database the database configuration
     * @return a PostgresConnection
     */
    protected PostgresConnection createConnection(UniqueDatabase database) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", database.getHost());
        properties.put("port", String.valueOf(database.getDatabasePort()));
        properties.put("user", database.getUsername());
        properties.put("password", database.getPassword());
        properties.put("dbname", database.getDatabaseName());
        return createConnection(properties);
    }

    /**
     * Creates a PostgresConnection from the given properties.
     *
     * @param properties the connection properties
     * @return a PostgresConnection
     */
    protected PostgresConnection createConnection(Map<String, String> properties) {
        Configuration config = Configuration.from(properties);
        return new PostgresConnection(JdbcConfiguration.adapt(config), "test-connection");
    }

    /**
     * Creates a PostgresConnection for the current container.
     *
     * @return a PostgresConnection
     */
    protected PostgresConnection createConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", getContainer().getHost());
        properties.put("port", String.valueOf(getContainer().getMappedPort(5432)));
        properties.put("user", getContainer().getUsername());
        properties.put("password", getContainer().getPassword());
        properties.put("dbname", getContainer().getDatabaseName());
        return createConnection(properties);
    }

    /**
     * Generates a unique replication slot name for testing.
     *
     * @return a unique slot name
     */
    public static String getSlotName() {
        final Random random = new Random();
        int id = random.nextInt(10000);
        return "flink_" + id;
    }

    /**
     * Initializes PostgreSQL tables from a SQL file using the current container.
     *
     * @param sqlFile the SQL file name (without .sql extension)
     */
    protected void initializePostgresTable(String sqlFile) {
        initializePostgresTable(getContainer(), sqlFile);
    }

    /**
     * Initializes PostgreSQL tables from a SQL file using the specified container.
     *
     * @param container the PostgreSQL container
     * @param sqlFile the SQL file name (without .sql extension)
     */
    protected void initializePostgresTable(PostgreSQLContainer<?> container, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile =
                PostgresVersionedTestBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getJdbcConnection(container);
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
                                            .split(";\n"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits for snapshot to start by checking sink size.
     *
     * @param sinkName the sink name
     * @throws InterruptedException if interrupted while waiting
     */
    protected void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            sleep(300);
        }
    }

    /**
     * Waits for sink to contain the expected results.
     *
     * @param sinkName the sink name
     * @param expected the expected results
     * @throws InterruptedException if interrupted while waiting
     */
    protected void waitForSinkResult(String sinkName, List<String> expected)
            throws InterruptedException {
        List<String> actual = TestValuesTableFactory.getResultsAsStrings(sinkName);
        actual = actual.stream().sorted().collect(Collectors.toList());
        while (actual.size() != expected.size() || !actual.equals(expected)) {
            actual =
                    TestValuesTableFactory.getResultsAsStrings(sinkName).stream()
                            .sorted()
                            .collect(Collectors.toList());
            sleep(1000);
        }
    }

    /**
     * Waits for sink to reach the expected size.
     *
     * @param sinkName the sink name
     * @param expectedSize the expected size
     * @throws InterruptedException if interrupted while waiting
     */
    protected void waitForSinkSize(String sinkName, int expectedSize) throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            sleep(100);
        }
    }

    /**
     * Gets the current size of the sink.
     *
     * @param sinkName the sink name
     * @return the sink size
     */
    protected int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    /**
     * Creates a mock PostgresSourceConfigFactory for testing.
     *
     * @param database the database configuration
     * @param schemaName the schema name
     * @param tableName the table name
     * @param splitSize the split size
     * @return a configured PostgresSourceConfigFactory
     */
    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database, String schemaName, String tableName, int splitSize) {
        return getMockPostgresSourceConfigFactory(
                database, schemaName, tableName, splitSize, false);
    }

    /**
     * Creates a mock PostgresSourceConfigFactory for testing with optional snapshot backfill.
     *
     * @param database the database configuration
     * @param schemaName the schema name
     * @param tableName the table name
     * @param splitSize the split size
     * @param skipSnapshotBackfill whether to skip snapshot backfill
     * @return a configured PostgresSourceConfigFactory
     */
    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database,
            String schemaName,
            String tableName,
            int splitSize,
            boolean skipSnapshotBackfill) {

        PostgresSourceConfigFactory postgresSourceConfigFactory = new PostgresSourceConfigFactory();
        postgresSourceConfigFactory.hostname(database.getHost());
        postgresSourceConfigFactory.port(database.getDatabasePort());
        postgresSourceConfigFactory.username(database.getUsername());
        postgresSourceConfigFactory.password(database.getPassword());
        postgresSourceConfigFactory.database(database.getDatabaseName());
        postgresSourceConfigFactory.schemaList(new String[] {schemaName});
        postgresSourceConfigFactory.tableList(schemaName + "." + tableName);
        postgresSourceConfigFactory.splitSize(splitSize);
        postgresSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        postgresSourceConfigFactory.setLsnCommitCheckpointsDelay(1);
        postgresSourceConfigFactory.decodingPluginName("pgoutput");
        return postgresSourceConfigFactory;
    }

    /**
     * Fetches a specified number of rows from an iterator.
     *
     * @param iter the row iterator
     * @param size the number of rows to fetch
     * @return a list of row strings
     */
    public static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    /**
     * Asserts that two lists contain the same elements in any order.
     *
     * @param expected the expected list
     * @param actual the actual list
     */
    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * Gets the PostgreSQL version of the current container.
     *
     * @return the PostgreSQL version string
     */
    protected String getPostgresVersion() {
        return getPostgresVersion(getContainer());
    }

    /**
     * Gets the PostgreSQL version of the specified container.
     *
     * @param container the PostgreSQL container
     * @return the PostgreSQL version string
     */
    protected String getPostgresVersion(PostgreSQLContainer<?> container) {
        try (Connection connection = getJdbcConnection(container);
                Statement statement = connection.createStatement()) {
            var resultSet = statement.executeQuery("SELECT version()");
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
            return "unknown";
        } catch (SQLException e) {
            LOG.warn("Failed to get PostgreSQL version", e);
            return "unknown";
        }
    }

    /**
     * Checks if the current PostgreSQL version supports a specific feature.
     *
     * @param majorVersion the minimum major version required
     * @return true if the version is supported
     */
    protected boolean isPostgresVersionAtLeast(int majorVersion) {
        return isPostgresVersionAtLeast(getContainer(), majorVersion);
    }

    /**
     * Checks if the specified PostgreSQL container version supports a specific feature.
     *
     * @param container the PostgreSQL container
     * @param majorVersion the minimum major version required
     * @return true if the version is supported
     */
    protected boolean isPostgresVersionAtLeast(PostgreSQLContainer<?> container, int majorVersion) {
        String version = getPostgresVersion(container);
        try {
            // Extract major version from version string like "PostgreSQL 14.x on ..."
            String[] parts = version.split(" ");
            if (parts.length >= 2) {
                String versionNumber = parts[1];
                int actualMajorVersion = Integer.parseInt(versionNumber.split("\\.")[0]);
                return actualMajorVersion >= majorVersion;
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse PostgreSQL version: {}", version, e);
        }
        return false;
    }
}
