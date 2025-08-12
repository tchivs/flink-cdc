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

package org.apache.flink.cdc.connectors.postgres.testutils;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * JUnit 5 extension that provides PostgreSQL version-specific test execution.
 *
 * <p>This extension processes {@link PostgresVersion} annotations and creates separate test
 * invocation contexts for each specified PostgreSQL version. Each context gets its own
 * PostgreSQLContainer instance with the appropriate version and configuration.
 */
public class PostgresVersionExtension implements TestTemplateInvocationContextProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresVersionExtension.class);
    private static final String DEFAULT_POSTGRES_VERSION = "14";
    private static final String DEFAULT_DB = "postgres";
    private static final String TEST_USER = "postgres";
    private static final String TEST_PASSWORD = "postgres";
    private static final String INTER_CONTAINER_POSTGRES_ALIAS = "postgres";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return getPostgresVersions(context).length > 0;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {
        PostgresVersion[] versions = getPostgresVersions(context);

        if (versions.length == 0) {
            // Provide default version if no annotation is present
            versions = new PostgresVersion[] {createDefaultPostgresVersion()};
        }

        return Arrays.stream(versions)
                .map(version -> new PostgresVersionInvocationContext(version));
    }

    /** Extracts PostgresVersion annotations from the test class or method. */
    private PostgresVersion[] getPostgresVersions(ExtensionContext context) {
        // Check method-level annotations first
        Method testMethod = context.getTestMethod().orElse(null);
        if (testMethod != null) {
            PostgresVersion[] methodVersions = getVersionsFromAnnotatedElement(testMethod);
            if (methodVersions.length > 0) {
                return methodVersions;
            }
        }

        // Fall back to class-level annotations
        Class<?> testClass = context.getTestClass().orElse(null);
        if (testClass != null) {
            return getVersionsFromAnnotatedElement(testClass);
        }

        return new PostgresVersion[0];
    }

    /** Extracts PostgresVersion annotations from an annotated element. */
    private PostgresVersion[] getVersionsFromAnnotatedElement(
            java.lang.reflect.AnnotatedElement element) {
        // Check for multiple versions using @PostgresVersions
        PostgresVersions versionsAnnotation = element.getAnnotation(PostgresVersions.class);
        if (versionsAnnotation != null) {
            return versionsAnnotation.value();
        }

        // Check for single version using @PostgresVersion
        PostgresVersion versionAnnotation = element.getAnnotation(PostgresVersion.class);
        if (versionAnnotation != null) {
            return new PostgresVersion[] {versionAnnotation};
        }

        return new PostgresVersion[0];
    }

    /** Creates a default PostgresVersion annotation for cases where none is specified. */
    private PostgresVersion createDefaultPostgresVersion() {
        return new PostgresVersion() {
            @Override
            public Class<? extends java.lang.annotation.Annotation> annotationType() {
                return PostgresVersion.class;
            }

            @Override
            public String version() {
                return DEFAULT_POSTGRES_VERSION;
            }

            @Override
            public String image() {
                return "";
            }

            @Override
            public boolean enableLogicalReplication() {
                return true;
            }

            @Override
            public int maxReplicationSlots() {
                return 20;
            }

            @Override
            public String[] additionalConfig() {
                return new String[0];
            }
        };
    }

    /** Test invocation context for a specific PostgreSQL version. */
    private static class PostgresVersionInvocationContext implements TestTemplateInvocationContext {

        private final PostgresVersion postgresVersion;
        private final String displayName;

        public PostgresVersionInvocationContext(PostgresVersion postgresVersion) {
            this.postgresVersion = postgresVersion;
            this.displayName = "PostgreSQL " + postgresVersion.version();
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return displayName;
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new PostgresContainerExtension(postgresVersion));
        }
    }

    /** Extension that manages the lifecycle of a PostgreSQLContainer for a specific version. */
    private static class PostgresContainerExtension
            implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

        private final PostgresVersion postgresVersion;
        private PostgreSQLContainer<?> container;
        private Network network;

        public PostgresContainerExtension(PostgresVersion postgresVersion) {
            this.postgresVersion = postgresVersion;
        }

        @Override
        public void beforeEach(ExtensionContext context) throws Exception {
            LOG.info(
                    "Starting PostgreSQL {} container for test: {}",
                    postgresVersion.version(),
                    context.getDisplayName());

            // Create network for this test
            network = Network.newNetwork();

            // Determine Docker image name
            String imageName =
                    postgresVersion.image().isEmpty()
                            ? "postgres:" + postgresVersion.version()
                            : postgresVersion.image();

            DockerImageName dockerImageName =
                    DockerImageName.parse(imageName).asCompatibleSubstituteFor("postgres");

            // Build container configuration
            List<String> command = new ArrayList<>();
            command.add("postgres");
            command.add("-c");
            command.add("fsync=off"); // Default for testing

            if (postgresVersion.enableLogicalReplication()) {
                command.add("-c");
                command.add("wal_level=logical");
            }

            command.add("-c");
            command.add("max_replication_slots=" + postgresVersion.maxReplicationSlots());

            // Add additional configuration
            for (String config : postgresVersion.additionalConfig()) {
                command.add("-c");
                command.add(config);
            }

            // Create and configure container
            container =
                    new PostgreSQLContainer<>(dockerImageName)
                            .withDatabaseName(DEFAULT_DB)
                            .withUsername(TEST_USER)
                            .withPassword(TEST_PASSWORD)
                            .withLogConsumer(new Slf4jLogConsumer(LOG))
                            .withNetwork(network)
                            .withNetworkAliases(INTER_CONTAINER_POSTGRES_ALIAS)
                            .withReuse(false)
                            .withCommand(command.toArray(new String[0]));

            // Start container
            container.start();

            // Store container in extension context for access by test methods
            ExtensionContext.Store store =
                    context.getStore(ExtensionContext.Namespace.create(getClass()));
            store.put("container", container);
            store.put("network", network);

            // Inject container and network into test instance if it's a PostgresVersionedTestBase
            Object testInstance = context.getTestInstance().orElse(null);
            if (testInstance
                    instanceof org.apache.flink.cdc.connectors.postgres.PostgresVersionedTestBase) {
                ((org.apache.flink.cdc.connectors.postgres.PostgresVersionedTestBase) testInstance)
                        .setContainerAndNetwork(container, network);
            }

            LOG.info("PostgreSQL {} container started successfully", postgresVersion.version());
        }

        @Override
        public void afterEach(ExtensionContext context) {
            LOG.info(
                    "Stopping PostgreSQL {} container for test: {}",
                    postgresVersion.version(),
                    context.getDisplayName());

            try {
                if (container != null) {
                    container.stop();
                }
            } catch (Exception e) {
                LOG.warn("Error stopping PostgreSQL container", e);
            }

            try {
                if (network != null) {
                    network.close();
                }
            } catch (Exception e) {
                LOG.warn("Error closing network", e);
            }

            LOG.info("PostgreSQL {} container stopped", postgresVersion.version());
        }

        @Override
        public boolean supportsParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType() == PostgreSQLContainer.class
                    || parameterContext.getParameter().getType() == Network.class;
        }

        @Override
        public Object resolveParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext) {
            ExtensionContext.Store store =
                    extensionContext.getStore(ExtensionContext.Namespace.create(getClass()));

            if (parameterContext.getParameter().getType() == PostgreSQLContainer.class) {
                return store.get("container");
            } else if (parameterContext.getParameter().getType() == Network.class) {
                return store.get("network");
            }

            throw new IllegalArgumentException(
                    "Unsupported parameter type: " + parameterContext.getParameter().getType());
        }
    }
}
