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

import org.apache.flink.cdc.connectors.postgres.PostgresVersionedTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for PostgresSqlQueries to verify the unified SQL query management functionality. */
@PostgresVersion()
class PostgresSqlQueriesTest extends PostgresVersionedTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSqlQueriesTest.class);

    private PostgresConnection connection;

    @BeforeEach
    void setUp()  {
        LOG.info("Setting up PostgresSqlQueries test");
        // Create connection
        connection = createConnection();
        LOG.info("Test setup completed");
    }

    @AfterEach
    void tearDown() {
        PostgresVersionStrategy.clearCache();
        PostgresVersionUtils.clearVersionCache();
        LOG.info("Test cleanup completed");
    }

    @TestTemplate
    void testGetQueryWithConnection()  {
        LOG.info("Testing getQuery with PostgreSQL connection");

        // Test getting partition table check query
        String partitionQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PARTITION_TABLE, connection);

        assertThat(partitionQuery).isNotNull();
        assertThat(partitionQuery).contains("SELECT c.relispartition");
        assertThat(partitionQuery).contains("FROM pg_class c");
        assertThat(partitionQuery).contains("JOIN pg_namespace n");
        assertThat(partitionQuery).contains("WHERE n.nspname = ? AND c.relname = ?");

        LOG.info("✓ Partition table query retrieved successfully: {}", partitionQuery);
    }

    @TestTemplate
    void testGetQueryWithVersionStrategy()  {
        LOG.info("Testing getQuery with explicit version strategy");

        // Test with V11_PLUS strategy
        String primaryKeyQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.GET_PRIMARY_KEY_COLUMNS,
                        PostgresVersionStrategy.V11_PLUS);

        assertThat(primaryKeyQuery).isNotNull();
        assertThat(primaryKeyQuery).contains("SELECT a.attname");
        assertThat(primaryKeyQuery).contains("FROM pg_index i");
        assertThat(primaryKeyQuery).contains("WHERE i.indisprimary = true");
        assertThat(primaryKeyQuery).contains("ORDER BY array_position");

        LOG.info("✓ Primary key query retrieved successfully: {}", primaryKeyQuery);
    }

    @TestTemplate
    void testAllQueryTypes()  {
        LOG.info("Testing all supported query types");

        PostgresSqlQueries.QueryType[] queryTypes = PostgresSqlQueries.QueryType.values();

        for (PostgresSqlQueries.QueryType queryType : queryTypes) {
            String query = PostgresSqlQueries.getQuery(queryType, connection);

            assertThat(query).isNotNull();
            assertThat(query).isNotEmpty();
            assertThat(query).contains("SELECT");
            assertThat(query).contains("FROM");

            LOG.debug(
                    "✓ Query type {} works: {}",
                    queryType,
                    query.substring(0, Math.min(50, query.length())));
        }

        LOG.info("✓ All {} query types are supported", queryTypes.length);
    }

    @TestTemplate
    void testQuerySupported()  {
        LOG.info("Testing query support detection");

        // Test supported query
        boolean isSupported =
                PostgresSqlQueries.isQuerySupported(
                        PostgresSqlQueries.QueryType.CHECK_PARTITION_TABLE, connection);
        assertThat(isSupported).isTrue();

        // Test with explicit strategy
        boolean isSupportedV11 =
                PostgresSqlQueries.isQuerySupported(
                        PostgresSqlQueries.QueryType.FIND_PARENT_TABLE,
                        PostgresVersionStrategy.V11_PLUS);
        assertThat(isSupportedV11).isTrue();

        LOG.info("✓ Query support detection works correctly");
    }

    @TestTemplate
    void testGetAllQueries()  {
        LOG.info("Testing getAllQueries functionality");

        // Test with connection
        Map<PostgresSqlQueries.QueryType, String> allQueries =
                PostgresSqlQueries.getAllQueries(connection);

        assertThat(allQueries).isNotNull();
        assertThat(allQueries).isNotEmpty();
        assertThat(allQueries).hasSize(PostgresSqlQueries.QueryType.values().length);

        // Verify all query types are present
        for (PostgresSqlQueries.QueryType queryType : PostgresSqlQueries.QueryType.values()) {
            assertThat(allQueries).containsKey(queryType);
            assertThat(allQueries.get(queryType)).isNotNull();
            assertThat(allQueries.get(queryType)).isNotEmpty();
        }

        LOG.info("✓ getAllQueries returned {} queries", allQueries.size());
    }

    @TestTemplate
    void testGetAllQueriesWithStrategy()  {
        LOG.info("Testing getAllQueries with explicit strategy");

        // Test with V10 strategy
        Map<PostgresSqlQueries.QueryType, String> v10Queries =
                PostgresSqlQueries.getAllQueries(PostgresVersionStrategy.V10);

        assertThat(v10Queries).isNotNull();
        assertThat(v10Queries).isNotEmpty();

        // Test with V11_PLUS strategy
        Map<PostgresSqlQueries.QueryType, String> v11Queries =
                PostgresSqlQueries.getAllQueries(PostgresVersionStrategy.V11_PLUS);

        assertThat(v11Queries).isNotNull();
        assertThat(v11Queries).isNotEmpty();

        // Both should have the same number of queries for now
        assertThat(v10Queries).hasSameSizeAs(v11Queries);

        LOG.info("✓ Strategy-specific queries work correctly");
    }

    @TestTemplate
    void testQueryManagementInfo()  {
        LOG.info("Testing query management info");

        String info = PostgresSqlQueries.getQueryManagementInfo();

        assertThat(info).isNotNull();
        assertThat(info).isNotEmpty();
        assertThat(info).contains("PostgresSqlQueries Management Info");
        assertThat(info).contains("V10:");
        assertThat(info).contains("V11_PLUS:");
        assertThat(info).contains("CHECK_PARTITION_TABLE");
        assertThat(info).contains("GET_PRIMARY_KEY_COLUMNS");

        LOG.info("✓ Query management info: {}", info);
    }

    @TestTemplate
    void testVersionStrategyDetection()  {
        LOG.info("Testing version strategy detection through queries");

        // Get the strategy that would be used for this connection
        PostgresVersionStrategy detectedStrategy =
                PostgresVersionStrategy.forConnection(connection);

        // Should be V11_PLUS for PostgreSQL 14
        assertThat(detectedStrategy).isEqualTo(PostgresVersionStrategy.V11_PLUS);

        // Verify queries work with detected strategy
        String query =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PARTITIONED_TABLE, detectedStrategy);

        assertThat(query).isNotNull();
        assertThat(query).contains("SELECT c.relkind = 'p' as is_partitioned");

        LOG.info("✓ Version strategy detection works: {}", detectedStrategy);
    }

    @TestTemplate
    void testErrorHandling()  {
        LOG.info("Testing error handling");

        // Test with null strategy - should throw exception
        assertThatThrownBy(
                        () ->
                                PostgresSqlQueries.getQuery(
                                        PostgresSqlQueries.QueryType.CHECK_PARTITION_TABLE,
                                        (PostgresVersionStrategy) null))
                .isInstanceOf(IllegalArgumentException.class);

        LOG.info("✓ Error handling works correctly");
    }

    @TestTemplate
    void testQueryConsistency()  {
        LOG.info("Testing query consistency across different access methods");

        // Get query through connection
        String queryViaConnection =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_CHILD_PARTITIONS, connection);

        // Get query through explicit strategy
        PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
        String queryViaStrategy =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_CHILD_PARTITIONS, strategy);

        // Should be the same
        assertThat(queryViaConnection).isEqualTo(queryViaStrategy);

        LOG.info("✓ Query consistency verified across access methods");
    }

    @TestTemplate
    void testQueryParameterPlaceholders()  {
        LOG.info("Testing query parameter placeholders");

        // All queries should have proper parameter placeholders
        Map<PostgresSqlQueries.QueryType, String> allQueries =
                PostgresSqlQueries.getAllQueries(connection);

        for (Map.Entry<PostgresSqlQueries.QueryType, String> entry : allQueries.entrySet()) {
            String query = entry.getValue();
            PostgresSqlQueries.QueryType queryType = entry.getKey();

            // Most queries should have parameters, but some may not (like schema-wide queries)
            long parameterCount = query.chars().filter(ch -> ch == '?').count();

            // Special handling for different query types
            switch (queryType) {
                case CHECK_PUBLICATION_EXISTS:
                    assertThat(parameterCount)
                            .as("Publication check should have 1 parameter")
                            .isEqualTo(1);
                    break;
                case GET_SCHEMA_TABLES:
                    // This query doesn't need parameters as it gets all tables
                    assertThat(parameterCount)
                            .as("Schema tables query should have 0 parameters")
                            .isEqualTo(0);
                    break;
                case FIND_PARTITIONED_TABLES_BY_FILTER:
                    // This query uses String.format, so no ? parameters
                    assertThat(parameterCount)
                            .as("Filtered partitioned tables query should have 0 ? parameters")
                            .isEqualTo(0);
                    break;
                case FIND_CHILD_PARTITIONS_ALT:
                    // This query should have 2 parameters (parent schema and table name)
                    assertThat(parameterCount)
                            .as("Alternative child partitions query should have 2 parameters")
                            .isEqualTo(2);
                    break;
                default:
                    // Most other queries should have at least 2 parameters (schema and table name)
                    assertThat(parameterCount)
                            .as("Query %s should have parameters", queryType)
                            .isGreaterThanOrEqualTo(2);
                    break;
            }

            LOG.debug("✓ Query {} has {} parameters", queryType, parameterCount);
        }

        LOG.info("✓ All queries have proper parameter placeholders");
    }

    @TestTemplate
    void testNewQueryTypes()  {
        LOG.info("Testing new query types added in extended refactoring");

        // Test publication existence check
        String publicationQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.CHECK_PUBLICATION_EXISTS, connection);
        assertThat(publicationQuery).isNotNull();
        assertThat(publicationQuery).contains("SELECT COUNT(1) FROM pg_publication");
        assertThat(publicationQuery).contains("WHERE pubname = ?");

        // Test partitioned tables by filter
        String partitionedTablesQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_PARTITIONED_TABLES_BY_FILTER, connection);
        assertThat(partitionedTablesQuery).isNotNull();
        assertThat(partitionedTablesQuery).contains("SELECT schemaname, tablename");
        assertThat(partitionedTablesQuery).contains("FROM pg_tables t");
        assertThat(partitionedTablesQuery).contains("c.relkind = 'p'");

        // Test schema tables query
        String schemaTablesQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.GET_SCHEMA_TABLES, connection);
        assertThat(schemaTablesQuery).isNotNull();
        assertThat(schemaTablesQuery)
                .contains("SELECT n.nspname AS schema_name, c.relname AS table_name");
        assertThat(schemaTablesQuery).contains("FROM pg_class c");
        assertThat(schemaTablesQuery).contains("c.relkind = 'r'");

        // Test alternative child partitions query
        String childPartitionsAltQuery =
                PostgresSqlQueries.getQuery(
                        PostgresSqlQueries.QueryType.FIND_CHILD_PARTITIONS_ALT, connection);
        assertThat(childPartitionsAltQuery).isNotNull();
        assertThat(childPartitionsAltQuery)
                .contains("SELECT n.nspname AS schema_name, c.relname AS table_name");
        assertThat(childPartitionsAltQuery).contains("FROM pg_class c");
        assertThat(childPartitionsAltQuery).contains("JOIN pg_inherits i ON c.oid = i.inhrelid");
        assertThat(childPartitionsAltQuery).contains("WHERE parent_ns.nspname = ?");
        assertThat(childPartitionsAltQuery).contains("AND parent.relname = ?");

        LOG.info("✓ All new query types work correctly");
    }

    @TestTemplate
    void testVersionUtilsOptimizations() {
        LOG.info("Testing PostgresVersionUtils optimizations");

        // Test version comparison
        int comparison = PostgresVersionUtils.compareVersion(connection, ServerVersion.v10);
        assertThat(comparison).isGreaterThanOrEqualTo(0); // Should be 10 or later
        LOG.info("✓ Version comparison works correctly");

        // Test additional version check methods
        boolean isV12Plus = PostgresVersionUtils.isServer12OrLater(connection);
        boolean isV96Plus = PostgresVersionUtils.isServer96OrLater(connection);
        boolean isV95Plus = PostgresVersionUtils.isServer95OrLater(connection);

        assertThat(isV96Plus).isTrue(); // Should be true for any modern PostgreSQL
        assertThat(isV95Plus).isTrue(); // Should be true for any modern PostgreSQL

        LOG.info("✓ All version check methods work correctly");
    }
}
