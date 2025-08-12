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

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify PostgreSQL version(s) for test execution.
 *
 * <p>This annotation can be used to run tests against specific PostgreSQL versions. It supports
 * both single and multiple version testing through the {@link Repeatable} mechanism.
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Single version
 * @PostgresVersion(version = "14")
 * class MyTest extends PostgresVersionedTestBase {
 *     // tests will run on PostgreSQL 14
 * }
 *
 * // Multiple versions
 * @PostgresVersion(version = "14")
 * @PostgresVersion(version = "15")
 * @PostgresVersion(version = "16")
 * class MyTest extends PostgresVersionedTestBase {
 *     // tests will run on PostgreSQL 14, 15, and 16
 * }
 * }</pre>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(PostgresVersions.class)
public @interface PostgresVersion {

    /**
     * The PostgreSQL version to use for testing.
     *
     * @return the PostgreSQL version (e.g., "14", "15", "16")
     */
    String version() default "14";

    /**
     * Optional custom Docker image name. If not specified, defaults to "postgres:{version}".
     *
     * @return the Docker image name
     */
    String image() default "";

    /**
     * Whether to enable WAL level logical replication. Defaults to true as it's required for CDC
     * functionality.
     *
     * @return true if logical replication should be enabled
     */
    boolean enableLogicalReplication() default true;

    /**
     * Maximum number of replication slots. Defaults to 20 to support multiple concurrent tests.
     *
     * @return the maximum number of replication slots
     */
    int maxReplicationSlots() default 20;

    /**
     * Additional PostgreSQL configuration parameters. Each string should be in the format
     * "parameter=value".
     *
     * @return array of additional configuration parameters
     */
    String[] additionalConfig() default {};
}
