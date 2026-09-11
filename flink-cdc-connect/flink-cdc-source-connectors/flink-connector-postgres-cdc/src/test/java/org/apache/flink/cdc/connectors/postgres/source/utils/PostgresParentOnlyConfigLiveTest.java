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

import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Temporary live check against the dev PostgreSQL 10 server: the parent-only capture list must make
 * the partition children visible to both the JDBC discovery path and the Debezium replication
 * filter (the exact place where partition changes used to be dropped), and the feature switch must
 * keep the list untouched when disabled. Not part of the regular suite: it needs a reachable server.
 */
class PostgresParentOnlyConfigLiveTest {

    private static final String HOST = "10.115.24.236";
    private static final String DATABASE = "aia_0705fujian";
    private static final String PARENT = "aia_0705fujian.public.aia_t_vcs_pjdb";
    private static final String CHILD_PLAIN = "public.aia_t_vcs_pjdb_202609";
    private static final String CHILD_QUALIFIED = "aia_0705fujian.public.aia_t_vcs_pjdb_202609";

    @Test
    void parentOnlyListReachesBothFilterShapes() throws Exception {
        PostgresSourceConfig config = config(true);
        String include = config.getDbzProperties().getProperty("table.include.list");
        assertThat(include).contains(CHILD_PLAIN).contains(CHILD_QUALIFIED);

        RelationalTableFilters filters = config.getDbzConnectorConfig().getTableFilters();
        assertThat(filters.dataCollectionFilter().isIncluded(new TableId(null, "public", "aia_t_vcs_pjdb_202609")))
                .as("replication-connection filter must accept the partition in schema.table shape")
                .isTrue();
    }

    @Test
    void disabledFeatureLeavesTheListUntouched() throws Exception {
        PostgresSourceConfig config = config(false);
        String include = config.getDbzProperties().getProperty("table.include.list");
        assertThat(include).isEqualTo(PARENT).doesNotContain("_202609");
    }

    private static PostgresSourceConfig config(boolean includePartitionedTables) {
        String password = System.getProperty("pg.password", "");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                !password.isEmpty(),
                "set -Dpg.password=<postgres password> to run this live check against the dev server");
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname(HOST);
        factory.port(5432);
        factory.database(DATABASE);
        factory.username("postgres");
        factory.password(password);
        factory.schemaList(new String[] {"public"});
        factory.tableList(new String[] {PARENT});
        factory.decodingPluginName("pgoutput");
        factory.slotName("aia_phase1_cdc_pipeline_v1");
        factory.setIncludePartitionedTables(includePartitionedTables);
        factory.setIncludeDatabaseInTableId(true);
        return factory.create(0);
    }
}
