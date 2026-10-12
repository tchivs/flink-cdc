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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.relational.TableId;

import java.util.Optional;

/**
 * Fails the stream when a table filtered out by Debezium turns out to be a partition of a
 * configured partition root, i.e. a partition attached after the partition routing was resolved.
 *
 * <p>Debezium's {@code table.include.list} is fixed when the source config is created, so the
 * changes of such a partition would be dropped silently. pgoutput sends the relation of a table
 * before its first change, so failing here loses nothing: the restarted source re-resolves the
 * routing and replays from the checkpointed position with the partition included.
 */
public class PartitionGuard {

    private final PostgresSourceConfig sourceConfig;

    public PartitionGuard(PostgresSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    // ponytail: one pg_inherits query per filtered relation per walsender session (pgoutput sends a
    // relation once per session and again after DDL); add a negative cache by relation id if
    // publications with many unrelated tables make startup slow.
    public void check(TableId filteredTable) {
        String schemaDotTable = filteredTable.schema() + "." + filteredTable.table();
        Optional<String> root =
                PostgresPartitionRouting.resolve(
                                String.format(
                                        "jdbc:postgresql://%s:%d/%s",
                                        sourceConfig.getHostname(),
                                        sourceConfig.getPort(),
                                        sourceConfig.getDatabaseList().get(0)),
                                sourceConfig.getUsername(),
                                sourceConfig.getPassword(),
                                sourceConfig.getTableList(),
                                true)
                        .parentOf(schemaDotTable);
        if (root.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Partition %s of partition root %s was attached after the partition "
                                    + "routing was resolved and is not captured; failing so that "
                                    + "the restart re-resolves partition routing.",
                            schemaDotTable, root.get()));
        }
    }
}
