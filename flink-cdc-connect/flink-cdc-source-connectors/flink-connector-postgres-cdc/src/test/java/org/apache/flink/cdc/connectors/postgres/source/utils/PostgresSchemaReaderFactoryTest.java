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
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresVersion;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PostgresSchemaReaderFactory to verify the factory pattern implementation and
 * version strategy selection.
 */
@PostgresVersion(version = "14")
class PostgresSchemaReaderFactoryTest extends PostgresVersionedTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresSchemaReaderFactoryTest.class);

    private PostgresSchemaReaderFactory factory;
    private PostgresConnection connection;
    private PostgresSourceConfig config;

    @BeforeEach
    void setUp()  {
        LOG.info("Setting up PostgresSchemaReaderFactory test");

        // Initialize factory
        factory = new PostgresSchemaReaderFactory();

        // Create connection
        connection = createConnection();

        // Create config
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(getContainer().getHost());
        configFactory.port(getContainer().getMappedPort(5432));
        configFactory.database(getContainer().getDatabaseName());
        configFactory.username(getContainer().getUsername());
        configFactory.password(getContainer().getPassword());
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList(new String[] {"public.test_table"});
        configFactory.splitSize(10);
        config = configFactory.create(0);

        LOG.info("Test setup completed");
    }

    @AfterEach
    void tearDown() {
        if (factory != null) {
            factory.clearCache();
        }
        PostgresVersionStrategy.clearCache();
        PostgresVersionUtils.clearVersionCache();
        LOG.info("Test cleanup completed");
    }

    @TestTemplate
    void testFactoryCreatesCorrectSchemaReader()  {
        LOG.info("Testing factory creates correct schema reader for PostgreSQL version");

        // Test factory method
        PostgresSchemaReader reader = factory.createReader(connection, config);

        assertThat(reader).isNotNull();
        assertThat(reader).isInstanceOf(CustomPostgresSchema.class);

        LOG.info("✓ Factory correctly created CustomPostgresSchema for PostgreSQL 14");
    }

    @TestTemplate
    void testStaticFactoryMethod()  {
        LOG.info("Testing static factory convenience method");

        // Test static convenience method
        PostgresSchemaReader reader = PostgresSchemaReaderFactory.create(connection, config);

        assertThat(reader).isNotNull();
        assertThat(reader).isInstanceOf(CustomPostgresSchema.class);

        LOG.info("✓ Static factory method works correctly");
    }

    @TestTemplate
    void testFactoryCaching()  {
        LOG.info("Testing factory caching mechanism");

        // Create first reader
        PostgresSchemaReader reader1 = factory.createReader(connection, config);

        // Create second reader with same parameters
        PostgresSchemaReader reader2 = factory.createReader(connection, config);

        // Should be the same cached instance
        assertThat(reader1).isSameAs(reader2);
        assertThat(factory.getCacheSize()).isEqualTo(1);

        LOG.info("✓ Factory caching works correctly, cache size: {}", factory.getCacheSize());
    }

    @TestTemplate
    void testVersionStrategyDetection()  {
        LOG.info("Testing version strategy detection");

        // Test version strategy detection
        PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);

        assertThat(strategy).isEqualTo(PostgresVersionStrategy.V11_PLUS);
        assertThat(strategy.supportsVersion(14)).isTrue();
        assertThat(strategy.supportsVersion(10)).isFalse();

        LOG.info("✓ Version strategy detection works correctly: {}", strategy);
    }

    @TestTemplate
    void testExplicitVersionStrategy()  {
        LOG.info("Testing explicit version strategy usage");

        // Test explicit strategy usage
        PostgresSchemaReader reader =
                factory.createReader(connection, config, PostgresVersionStrategy.V11_PLUS);

        assertThat(reader).isNotNull();
        assertThat(reader).isInstanceOf(CustomPostgresSchema.class);

        LOG.info("✓ Explicit version strategy works correctly");
    }

    @TestTemplate
    void testCacheClearance()  {
        LOG.info("Testing cache clearance functionality");

        // Create some cached entries
        factory.createReader(connection, config);
        assertThat(factory.getCacheSize()).isEqualTo(1);

        // Clear cache
        factory.clearCache();
        assertThat(factory.getCacheSize()).isEqualTo(0);

        LOG.info("✓ Cache clearance works correctly");
    }

    @TestTemplate
    void testFactoryInfo()  {
        LOG.info("Testing factory information retrieval");

        // Create a reader to populate cache
        factory.createReader(connection, config);

        // Get factory info
        String info = factory.getFactoryInfo();

        assertThat(info).isNotNull();
        assertThat(info).contains("PostgresSchemaReaderFactory");
        assertThat(info).contains("cacheSize=1");

        LOG.info("✓ Factory info: {}", info);
    }

    @TestTemplate
    void testVersionUtilsCaching() {
        LOG.info("Testing PostgresVersionUtils caching");

        // Test version detection caching
        boolean isV11Plus1 = PostgresVersionUtils.isServer11OrLater(connection);
        boolean isV11Plus2 = PostgresVersionUtils.isServer11OrLater(connection);

        assertThat(isV11Plus1).isTrue();
        assertThat(isV11Plus2).isTrue();
        assertThat(PostgresVersionUtils.getVersionCacheSize()).isGreaterThan(0);
    }
}
