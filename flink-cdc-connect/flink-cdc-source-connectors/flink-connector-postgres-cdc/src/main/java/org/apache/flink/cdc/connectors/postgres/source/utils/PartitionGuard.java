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

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Fails the stream when the partition routing resolved at startup no longer matches the catalog, so
 * that the restarted source re-resolves it.
 *
 * <p>Debezium's {@code table.include.list} and the routing are fixed when the source config is
 * created. Both checks run when pgoutput sends a relation, which it does before the first change of
 * a table in a session and again after a catalog change of that table, ATTACH and DETACH included:
 *
 * <ul>
 *   <li>{@link #checkFiltered}: a filtered table that is a partition of a configured root was
 *       attached later; its changes would be dropped silently.
 *   <li>{@link #checkRouted}: a routed partition that is no longer a partition of its root was
 *       detached; its changes would be written to the root.
 * </ul>
 *
 * <p>Both fail closed: a catalog that cannot be read must not let a change through.
 */
public class PartitionGuard {

    private final PostgresPartitionRouting routing;
    private final String database;
    private final List<String> configuredTables;
    private final Callable<Connection> connection;

    /** Queries the catalog on the given connection of the stream, it opens no connection itself. */
    public PartitionGuard(PostgresSourceConfig sourceConfig, JdbcConnection jdbcConnection) {
        this(
                sourceConfig.getPartitionRouting(),
                sourceConfig.getDatabaseList().get(0),
                sourceConfig.getTableList(),
                jdbcConnection::connection);
    }

    PartitionGuard(
            PostgresPartitionRouting routing,
            String database,
            List<String> configuredTables,
            Callable<Connection> connection) {
        this.routing = routing;
        this.database = database;
        this.configuredTables = configuredTables;
        this.connection = connection;
    }

    /** Checks a table filtered out by Debezium. */
    public void checkFiltered(TableId filteredTable) {
        Optional<String> root = configuredRootOf(filteredTable);
        if (root.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Partition %s of partition root %s was attached after the partition "
                                    + "routing was resolved and is not captured; failing so that "
                                    + "the restart re-resolves partition routing.",
                            schemaDotTable(filteredTable), root.get()));
        }
    }

    /** Checks a table captured by Debezium, only routed partitions are looked up. */
    public void checkRouted(TableId capturedTable) {
        Optional<String> expectedRoot = routing.parentOf(schemaDotTable(capturedTable));
        if (!expectedRoot.isPresent()) {
            return;
        }
        Optional<String> root = configuredRootOf(capturedTable);
        if (!root.equals(expectedRoot)) {
            throw new IllegalStateException(
                    String.format(
                            "Table %s is no longer a partition of partition root %s, it was "
                                    + "detached after the partition routing was resolved; failing "
                                    + "so that the restart re-resolves partition routing instead "
                                    + "of writing its changes to %s.",
                            schemaDotTable(capturedTable), expectedRoot.get(), expectedRoot.get()));
        }
    }

    private Optional<String> configuredRootOf(TableId table) {
        try {
            return PostgresPartitionRouting.configuredRootOf(
                    connection.call(), database, configuredTables, table.schema(), table.table());
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot verify the partition routing of table %s; failing so that "
                                    + "its changes are neither dropped nor misrouted silently.",
                            schemaDotTable(table)),
                    e);
        }
    }

    private static String schemaDotTable(TableId table) {
        return table.schema() + "." + table.table();
    }
}
