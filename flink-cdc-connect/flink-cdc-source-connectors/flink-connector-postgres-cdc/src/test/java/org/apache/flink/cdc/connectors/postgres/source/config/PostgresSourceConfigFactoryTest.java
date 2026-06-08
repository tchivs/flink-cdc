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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/** Unit tests for {@link PostgresSourceConfigFactory}. */
class PostgresSourceConfigFactoryTest {

    @Test
    void includePartitionedTablesInternalFlagIsNotOverriddenByDebeziumProperties() {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(
                PostgresSourceConfigFactory.FLINK_CDC_INCLUDE_PARTITIONED_TABLES, "false");
        dbzProperties.setProperty("include.partitioned.tables", "false");

        PostgresSourceConfigFactory factory = baseFactory();
        factory.setIncludePartitionedTables(true);
        factory.debeziumProperties(dbzProperties);

        PostgresSourceConfig config = factory.create(0);

        Assertions.assertThat(config.includePartitionedTables()).isTrue();
        Assertions.assertThat(
                        config.getDbzConfiguration()
                                .getString(
                                        PostgresSourceConfigFactory
                                                .FLINK_CDC_INCLUDE_PARTITIONED_TABLES))
                .isEqualTo("true");
    }

    private static PostgresSourceConfigFactory baseFactory() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(5432);
        factory.username("postgres");
        factory.password("postgres");
        factory.database("inventory");
        factory.schemaList(new String[] {"public"});
        factory.tableList("public.products");
        factory.slotName("flink");
        return factory;
    }
}
