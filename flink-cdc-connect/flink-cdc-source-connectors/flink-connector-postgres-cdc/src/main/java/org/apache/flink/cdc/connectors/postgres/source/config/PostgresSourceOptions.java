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

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link PostgresSourceBuilder.PostgresIncrementalSource}. */
public class PostgresSourceOptions extends JdbcSourceOptions {

    public static final ConfigOption<Integer> PG_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Integer port number of the PostgreSQL database server.");

    public static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("decoderbufs")
                    .withDescription(
                            "The name of the Postgres logical decoding plug-in installed on the server.\n"
                                    + "Supported values are decoderbufs and pgoutput.");

    public static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the PostgreSQL logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses this slot "
                                    + "to stream events to the connector that you are configuring.");

    public static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(DebeziumChangelogMode.class)
                    .defaultValue(DebeziumChangelogMode.ALL)
                    .withDescription(
                            "The changelog mode used for encoding streaming changes.\n"
                                    + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                    + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key. It can be used for tables with primary keys when replica identity FULL is not an option.");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock before snapshot reading.");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available replication slot offsets");

    public static final ConfigOption<Integer> SCAN_LSN_COMMIT_CHECKPOINTS_DELAY =
            ConfigOptions.key("scan.lsn-commit.checkpoints-num-delay")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of checkpoint delays before starting to commit the LSN offsets.\n"
                                    + "By setting this to higher value, the offset that is consumed by global slot will be "
                                    + "committed after multiple checkpoint delays instead of after each checkpoint completion.\n"
                                    + "This allows continuous recycle of log files in stream phase.");

    public static final ConfigOption<Boolean> SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED =
            ConfigOptions.key("scan.include-partitioned-tables.enabled")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription(
                            "Enable reading from partitioned table via partition root.\n"
                                    + "If enabled:\n"
                                    + "(1) PUBLICATION must be created beforehand with parameter publish_via_partition_root=true\n"
                                    + "(2) Table list (regex or predefined list) should only match the parent table name, if table list matches both parent and child tables, snapshot data will be read twice.");

    public static final ConfigOption<Duration> PG10_PUBLICATION_POLL_INTERVAL =
            ConfigOptions.key("scan.pg10.publication.poll.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Polling interval used by PG10 runtime child-partition publication discovery."
                                    + " This only applies when partitioned tables are enabled and a pgoutput publication is configured.");

    public static final ConfigOption<Duration> PG10_STARTUP_FAST_POLL_INTERVAL =
            ConfigOptions.key("scan.pg10.startup.fast-poll.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Optional shorter polling interval used during the initial PG10 runtime"
                                    + " discovery window after the streaming session starts.");

    public static final ConfigOption<Duration> PG10_STARTUP_FAST_POLL_DURATION =
            ConfigOptions.key("scan.pg10.startup.fast-poll.duration")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "How long the optional PG10 startup fast-poll interval should remain"
                                    + " active. Set to 0 to disable startup fast-polling.");

    public static final ConfigOption<Boolean> PG10_CHILD_PARTITION_BACKFILL_ENABLED =
            ConfigOptions.key("scan.pg10.child-partition.backfill.enabled")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription(
                            "Whether PG10 runtime-discovered child partitions should use the future"
                                    + " compensating backfill flow after they become restart-eligible."
                                    + " Disabled by default to preserve current semantics.");

    public static final ConfigOption<Boolean> TABLE_ID_INCLUDE_DATABASE =
            ConfigOptions.key("table-id.include-database")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to include database in the generated Table ID.\n"
                                    + "If set to true, the Table ID will be in the format (database, schema, table).\n"
                                    + "If set to false, the Table ID will be in the format (schema, table). Defaults to false.");
}
