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
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
    public static final Pattern BLOCK_COMMENT_PATTERN =
            Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    public static final Pattern LINE_COMMENT_PATTERN = Pattern.compile("--.*$", Pattern.MULTILINE);
    public static final String DEFAULT_DB = "postgres";
    public static final String TEST_USER = "postgres";
    public static final String TEST_PASSWORD = "postgres";

    // Instance variables for per-test container management
    protected PostgreSQLContainer<?> currentContainer;
    protected Network currentNetwork;

    // Test lifecycle management
    protected UniqueDatabase testDatabase;
    protected String testSlotName;

    // Thread-safe counter for generating unique slot names
    private static final AtomicLong SLOT_COUNTER = new AtomicLong(0);

    // Constants for slot name generation to ensure uniqueness while staying under PostgreSQL's 63
    // character limit
    private static final long TIMESTAMP_MODULO = 1000000L; // Use last 6 digits of timestamp
    private static final long THREAD_ID_MODULO = 1000L; // Use last 3 digits of thread ID
    private static final long COUNTER_MODULO = 10000L; // Use last 4 digits of counter

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
     * Gets a JDBC connection to the current PostgreSQL container. If a test database is available,
     * connects to it; otherwise connects to the default database.
     *
     * @return a JDBC connection
     * @throws SQLException if connection fails
     */
    protected Connection getJdbcConnection() throws SQLException {
        if (testDatabase != null) {
            return getJdbcConnection(getContainer(), testDatabase.getDatabaseName());
        }
        return getJdbcConnection(getContainer());
    }

    /**
     * Gets a JDBC connection with search_path set to the test schema.
     *
     * @return a JDBC connection with search_path configured
     * @throws SQLException if connection fails
     */
    protected Connection getJdbcConnectionWithSearchPath() throws SQLException {
        Connection connection = getJdbcConnection();
        try (Statement stmt = connection.createStatement()) {
            String schemaName = getSchemaFromAnnotation();
            stmt.execute("SET search_path TO " + schemaName);
        } catch (Exception e) {
            LOG.warn("Failed to set search_path: {}", e.getMessage());
        }
        return connection;
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
     * Generates a unique replication slot name for testing with enhanced concurrency safety.
     *
     * <p>The generated slot name format: flink_{timestamp}_{threadId}_{counter} This ensures
     * uniqueness across:
     *
     * <ul>
     *   <li>Different test runs (timestamp)
     *   <li>Concurrent threads (threadId)
     *   <li>Multiple calls within the same thread (counter)
     * </ul>
     *
     * @return a unique slot name safe for concurrent usage
     */
    public static String getSlotName() {
        long timestamp = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long counter = SLOT_COUNTER.incrementAndGet();

        // Format: flink_timestamp_threadId_counter
        // Keep it under PostgreSQL's 63 character limit for identifiers
        return String.format(
                "flink_%d_%d_%d",
                timestamp % TIMESTAMP_MODULO,
                threadId % THREAD_ID_MODULO,
                counter % COUNTER_MODULO);
    }

    /**
     * Generates a unique replication slot name with custom prefix for testing.
     *
     * @param prefix the prefix for the slot name
     * @return a unique slot name with the specified prefix
     */
    public static String getSlotName(String prefix) {
        if (prefix == null || prefix.trim().isEmpty()) {
            return getSlotName();
        }

        long timestamp = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long counter = SLOT_COUNTER.incrementAndGet();

        // Sanitize prefix to ensure it's a valid PostgreSQL identifier
        String sanitizedPrefix = prefix.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();

        return String.format(
                "%s_%d_%d_%d",
                sanitizedPrefix,
                timestamp % TIMESTAMP_MODULO,
                threadId % THREAD_ID_MODULO,
                counter % COUNTER_MODULO);
    }

    /**
     * Initializes PostgreSQL tables from a SQL file using the current container.
     *
     * @param sqlFiles the SQL files name (without .sql extension)
     */
    protected void initializePostgresTable(String... sqlFiles) {
        initializePostgresTable(getContainer(), sqlFiles);
    }

    /**
     * Initializes PostgreSQL tables using version-specific DDL files with fallback. First tries
     * ddl/{version}/{sqlFile}.sql, then falls back to ddl/{sqlFile}.sql.
     *
     * @param container the PostgreSQL container
     * @param sqlFiles the SQL files name (without .sql extension)
     */
    protected void initializePostgresTable(PostgreSQLContainer<?> container, String... sqlFiles) {
        for (String sqlFile : sqlFiles) {
            URL ddlTestFile = findVersionSpecificDdlFile(sqlFile);
            Assertions.assertThat(ddlTestFile)
                    .withFailMessage("Cannot locate DDL file for " + sqlFile)
                    .isNotNull();

            try (Connection connection = getJdbcConnection(container);
                    Statement statement = connection.createStatement()) {
                final List<String> statements = parseSqlStatementsLegacy(ddlTestFile);
                for (String stmt : statements) {
                    statement.execute(stmt);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to initialize PostgreSQL table from file: " + sqlFile, e);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read or parse DDL file: " + sqlFile, e);
            }
        }
    }

    protected static List<String> parseSqlStatements(String sqlContent) {
        if (StringUtils.isEmpty(sqlContent)) {
            return Collections.emptyList();
        }
        List<String> statements = new ArrayList<>();
        StringBuilder currentStatement = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inDollarQuote = false;
        String dollarQuoteTag = null;
        boolean inBlockComment = false;
        boolean inLineComment = false;

        char[] chars = sqlContent.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            char nextCh = (i + 1 < chars.length) ? chars[i + 1] : '\0';

            // Handle block comments
            if (!inSingleQuote
                    && !inDoubleQuote
                    && !inDollarQuote
                    && !inBlockComment
                    && !inLineComment) {
                if (ch == '/' && nextCh == '*') {
                    inBlockComment = true;
                    i++; // Skip the '*'
                    continue;
                }
            }

            if (inBlockComment) {
                if (ch == '*' && nextCh == '/') {
                    inBlockComment = false;
                    i++; // Skip the '/'
                }
                continue;
            }

            // Handle line comments
            if (!inSingleQuote && !inDoubleQuote && !inDollarQuote && !inLineComment) {
                if (ch == '-' && nextCh == '-') {
                    inLineComment = true;
                    i++; // Skip the second '-'
                    continue;
                }
            }

            if (inLineComment) {
                if (ch == '\n') {
                    inLineComment = false;
                    currentStatement.append(ch);
                }
                continue;
            }

            // Handle dollar quotes
            if (!inSingleQuote && !inDoubleQuote && ch == '$') {
                if (inDollarQuote) {
                    // Check if this might be the end of dollar quote
                    String potentialTag = extractDollarQuoteTag(chars, i);
                    if (potentialTag != null && potentialTag.equals(dollarQuoteTag)) {
                        currentStatement.append(ch);
                        for (int j = 1; j < potentialTag.length(); j++) {
                            currentStatement.append(chars[i + j]);
                        }
                        i += potentialTag.length() - 1;
                        inDollarQuote = false;
                        dollarQuoteTag = null;
                        continue;
                    }
                } else {
                    // Check if this is the start of a dollar quote
                    String tag = extractDollarQuoteTag(chars, i);
                    if (tag != null) {
                        inDollarQuote = true;
                        dollarQuoteTag = tag;
                        currentStatement.append(ch);
                        for (int j = 1; j < tag.length(); j++) {
                            currentStatement.append(chars[i + j]);
                        }
                        i += tag.length() - 1;
                        continue;
                    }
                }
            }

            // Handle single quotes
            if (!inDoubleQuote && !inDollarQuote && ch == '\'') {
                // Check for escaped quote
                if (inSingleQuote && nextCh == '\'') {
                    currentStatement.append(ch).append(nextCh);
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
            }

            // Handle double quotes
            if (!inSingleQuote && !inDollarQuote && ch == '"') {
                inDoubleQuote = !inDoubleQuote;
            }

            // Handle statement separator
            if (!inSingleQuote && !inDoubleQuote && !inDollarQuote && ch == ';') {
                currentStatement.append(ch);
                String stmt = currentStatement.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                currentStatement = new StringBuilder();
                continue;
            }

            currentStatement.append(ch);
        }

        // Add the last statement if any
        String lastStmt = currentStatement.toString().trim();
        if (!lastStmt.isEmpty()) {
            statements.add(lastStmt);
        }

        return statements;
    }

    /**
     * Extracts a dollar-quote tag from the character array starting at the given index.
     *
     * <p>Dollar quotes in PostgreSQL have the format: $tag$ where tag is optional. Examples: $$,
     * $body$, $function$
     *
     * @param chars the character array to parse
     * @param startIndex the index where the dollar quote starts
     * @return the complete dollar quote tag (e.g., "$$", "$tag$"), or null if not a valid dollar
     *     quote
     */
    private static String extractDollarQuoteTag(char[] chars, int startIndex) {
        if (startIndex >= chars.length || chars[startIndex] != '$') {
            return null;
        }

        StringBuilder tag = new StringBuilder();
        tag.append('$');

        int i = startIndex + 1;

        // Read the tag content (alphanumeric and underscore)
        while (i < chars.length && (Character.isLetterOrDigit(chars[i]) || chars[i] == '_')) {
            tag.append(chars[i]);
            i++;
        }

        // Check if it ends with $
        if (i < chars.length && chars[i] == '$') {
            tag.append('$');
            return tag.toString();
        }

        return null;
    }

    /**
     * Parses SQL statements from a file using the legacy line-by-line approach. This method is kept
     * for backward compatibility with existing SQL files.
     *
     * @param sqlFile the SQL file URL
     * @return list of parsed SQL statements
     * @throws Exception if file reading or parsing fails
     */
    protected static List<String> parseSqlStatementsLegacy(URL sqlFile) throws Exception {
        return Arrays.stream(
                        Files.readAllLines(Paths.get(sqlFile.toURI())).stream()
                                .map(String::trim)
                                .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                .map(
                                        x -> {
                                            final Matcher m = COMMENT_PATTERN.matcher(x);
                                            return m.matches() ? m.group(1) : x;
                                        })
                                .collect(Collectors.joining("\n"))
                                .split(";\n"))
                .collect(Collectors.toList());
    }

    /**
     * Finds version-specific DDL file with fallback mechanism.
     *
     * @param sqlFile the SQL file name (without .sql extension)
     * @return URL to the DDL file, or null if not found
     */
    private URL findVersionSpecificDdlFile(String sqlFile) {
        String version = getCurrentPostgresVersion();

        // First try version-specific path: ddl/{version}/{sqlFile}.sql
        if (version != null && !version.isEmpty()) {
            String versionSpecificFile = String.format("ddl/%s/%s.sql", version, sqlFile);
            URL versionSpecificUrl =
                    PostgresVersionedTestBase.class
                            .getClassLoader()
                            .getResource(versionSpecificFile);
            if (versionSpecificUrl != null) {
                return versionSpecificUrl;
            }
        }

        // Fallback to generic path: ddl/{sqlFile}.sql
        String genericFile = String.format("ddl/%s.sql", sqlFile);
        URL genericUrl = PostgresVersionedTestBase.class.getClassLoader().getResource(genericFile);
        if (genericUrl != null) {
            return genericUrl;
        }

        return null;
    }

    /**
     * Gets the PostgreSQL version from the current annotation.
     *
     * @return the PostgreSQL version string, or null if not available
     */
    private String getCurrentPostgresVersion() {
        Class<?> testClass = this.getClass();
        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion annotation =
                testClass.getAnnotation(
                        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion.class);

        if (annotation != null) {
            String[] versions = annotation.versions();
            return versions.length > 0 ? versions[0] : null;
        }

        // Check for repeated annotations
        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersions annotations =
                testClass.getAnnotation(
                        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersions.class);
        if (annotations != null && annotations.value().length > 0) {
            String[] versions = annotations.value()[0].versions();
            return versions.length > 0 ? versions[0] : null;
        }

        return null;
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
     * Creates a mock PostgresSourceConfigFactory for testing.
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @param splitSize the split size
     * @return a configured PostgresSourceConfigFactory
     */
    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            String schemaName, String tableName, int splitSize) {
        return getMockPostgresSourceConfigFactory(
                this.getTestDatabase(), schemaName, tableName, splitSize, false);
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

    /**
     * Gets the schema name from PostgresVersion annotation.
     *
     * @return the schema name from annotation
     * @throws IllegalStateException if no schema is configured in the annotation
     */
    private String getSchemaFromAnnotation() {
        Class<?> testClass = this.getClass();
        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion annotation =
                testClass.getAnnotation(
                        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion.class);

        if (annotation != null && !annotation.schema().isEmpty()) {
            return annotation.schema();
        }

        // Check for repeated annotations
        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersions annotations =
                testClass.getAnnotation(
                        org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersions.class);
        if (annotations != null && annotations.value().length > 0) {
            org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion firstAnnotation =
                    annotations.value()[0];
            if (!firstAnnotation.schema().isEmpty()) {
                return firstAnnotation.schema();
            }
        }

        throw new IllegalStateException(
                "No schema configured in @PostgresVersion annotation. "
                        + "Please specify schema() value in your @PostgresVersion annotation or use UniqueDatabase constructor directly.");
    }

    /**
     * Automatically sets up test resources before each test method. Creates and initializes the
     * database using schema from annotation if configured.
     */
    @BeforeEach
    protected void setUp() {
        doAutomaticSetup();
    }

    /**
     * Automatically cleans up test resources after each test method. Drops replication slot and
     * database if they were created.
     */
    @AfterEach
    protected void tearDown() throws SQLException {
        doAutomaticCleanup();
    }

    /** Performs the automatic database setup. */
    protected void doAutomaticSetup() {
        try {
            // Try to create database with schema from annotation
            String schemaName = getSchemaFromAnnotation();
            testDatabase = createVersionAwareUniqueDatabase(schemaName);
            testDatabase.createAndInitialize();
            testSlotName = getSlotName();
            LOG.info(
                    "Test setup completed with schema: {} on database: {}",
                    schemaName,
                    testDatabase.getDatabaseName());
        } catch (IllegalStateException e) {
            // Schema not configured in annotation, skip automatic setup
            if (e.getMessage() != null && e.getMessage().contains("No schema configured")) {
                LOG.debug("Skipping automatic setup: {}", e.getMessage());
            } else {
                throw e;
            }
        }
    }

    /** Creates a UniqueDatabase that uses version-specific DDL files. */
    private UniqueDatabase createVersionAwareUniqueDatabase(String schemaName) {
        return new UniqueDatabase(
                getContainer(), DEFAULT_DB, schemaName, TEST_USER, TEST_PASSWORD) {

            @Override
            public void createAndInitialize() {
                try {
                    // Create the database first (copied from UniqueDatabase)
                    try (Connection connection = getJdbcConnection(getContainer(), DEFAULT_DB)) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute("CREATE DATABASE " + getDatabaseName());
                        }
                    }

                    // Find version-specific DDL file
                    URL ddlTestFile = findVersionSpecificDdlFile(schemaName);
                    if (ddlTestFile == null) {
                        throw new RuntimeException(
                                "Cannot locate DDL file for schema: " + schemaName);
                    }

                    // Execute the version-specific DDL
                    try (Connection connection =
                                    getJdbcConnection(getContainer(), getDatabaseName());
                            Statement statement = connection.createStatement()) {

                        // Read and parse the SQL content
                        String sqlContent = Files.readString(Paths.get(ddlTestFile.toURI()));
                        final List<String> statements = parseSqlStatements(sqlContent);

                        for (String stmt : statements) {
                            statement.execute(stmt);
                        }

                        // run an analyze to collect the statistics about tables
                        statement.execute("analyze");
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Failed to create and initialize version-aware database", e);
                }
            }
        };
    }

    /** Performs the automatic cleanup. */
    protected void doAutomaticCleanup() {
        if (testSlotName != null) {
            try {
                if (testDatabase != null) {
                    testDatabase.removeSlot(testSlotName);
                }
            } catch (Exception e) {
                LOG.warn("Failed to drop replication slot {}: {}", testSlotName, e.getMessage());
            } finally {
                testSlotName = null;
            }
        }

        if (testDatabase != null) {
            try {
                // Use UniqueDatabase's built-in cleanup - it drops the schema
                testDatabase.dropDatabase();
            } catch (Exception e) {
                LOG.warn("Failed to drop test database: {}", e.getMessage());
            } finally {
                testDatabase = null;
            }
        }
    }

    /**
     * Gets the automatically created test database.
     *
     * @return the test database instance
     * @throws IllegalStateException if no database was created automatically
     */
    protected UniqueDatabase getTestDatabase() {
        if (testDatabase == null) {
            throw new IllegalStateException(
                    "No test database available. Either configure schema() in @PostgresVersion annotation "
                            + "for automatic setup, or create UniqueDatabase manually.");
        }
        return testDatabase;
    }

    /**
     * Gets the automatically created test slot name.
     *
     * @return the test slot name
     * @throws IllegalStateException if no slot was created automatically
     */
    protected String getTestSlotName() {
        if (testSlotName == null) {
            throw new IllegalStateException(
                    "No test slot available. Either configure schema() in @PostgresVersion annotation "
                            + "for automatic setup, or create slot manually.");
        }
        return testSlotName;
    }
}
