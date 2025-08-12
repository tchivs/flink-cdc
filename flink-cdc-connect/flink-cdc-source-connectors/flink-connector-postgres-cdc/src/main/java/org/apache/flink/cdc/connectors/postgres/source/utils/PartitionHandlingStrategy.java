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

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Strategy for handling partition tables based on PostgreSQL version capabilities. This class
 * encapsulates the complex logic for processing partition tables in different PostgreSQL versions.
 *
 * <p>Different PostgreSQL versions have different levels of support for partition tables:
 *
 * <ul>
 *   <li>PostgreSQL 9.x and earlier: No declarative partitioning support
 *   <li>PostgreSQL 10-12: Partitioned tables cannot be directly added to publications, child
 *       partitions must be added individually
 *   <li>PostgreSQL 13+: Partitioned tables can be directly added to publications
 * </ul>
 */
public abstract class PartitionHandlingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHandlingStrategy.class);

    /**
     * Creates the appropriate partition handling strategy based on the PostgreSQL connection
     * version.
     *
     * @param connection the PostgreSQL connection
     * @return the appropriate strategy for the server version
     */
    public static PartitionHandlingStrategy forConnection(PostgresConnection connection) {
        if (PostgresVersionUtils.isServer13OrLater(connection)) {
            return new DirectPartitionTableStrategy();
        } else if (PostgresVersionUtils.isServer10OrLater(connection)) {
            return new ChildPartitionExpansionStrategy(connection);
        } else {
            return new NoPartitionSupportStrategy();
        }
    }

    /**
     * Processes the given set of tables according to this strategy's partition handling approach.
     *
     * @param originalTables the original set of tables with type information to process
     * @return the processed set of table IDs with partition handling applied
     * @throws SQLException if database operation fails
     */
    public abstract Set<TableId> processPartitionedTables(Set<TableInfo> originalTables)
            throws SQLException;

    /**
     * Gets a description of this strategy.
     *
     * @return strategy description
     */
    public abstract String getDescription();

    /** Strategy for PostgreSQL 13+ that supports direct partitioned table handling. */
    private static class DirectPartitionTableStrategy extends PartitionHandlingStrategy {

        @Override
        public Set<TableId> processPartitionedTables(Set<TableInfo> originalTables) {
            LOG.info(
                    "PostgreSQL 13+ detected. Partitioned tables can be added directly to publication.");
            return originalTables.stream()
                    .map(TableInfo::getTableId)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        @Override
        public String getDescription() {
            return "Direct partitioned table support (PostgreSQL 13+)";
        }
    }

    /** Strategy for PostgreSQL 10-12 that requires child partition expansion. */
    private static class ChildPartitionExpansionStrategy extends PartitionHandlingStrategy {

        private final PostgresConnection connection;

        ChildPartitionExpansionStrategy(PostgresConnection connection) {
            this.connection = connection;
        }

        @Override
        public Set<TableId> processPartitionedTables(Set<TableInfo> originalTables)
                throws SQLException {
            LOG.info(
                    "PostgreSQL 10-12 detected. Will discover and add child partitions for partitioned tables.");

            Set<TableId> finalTables = new LinkedHashSet<>();
            Set<TableId> partitionedTableIds = new LinkedHashSet<>();

            // Separate partitioned tables from regular tables using type information
            for (TableInfo tableInfo : originalTables) {
                if (tableInfo.isPartitionedTable()) {
                    partitionedTableIds.add(tableInfo.getTableId());
                } else {
                    finalTables.add(tableInfo.getTableId());
                }
            }

            if (!partitionedTableIds.isEmpty()) {
                LOG.info(
                        "Found {} partitioned tables: {}",
                        partitionedTableIds.size(),
                        partitionedTableIds.stream()
                                .map(TableId::toString)
                                .collect(Collectors.joining(", ")));

                // Add child partitions for each partitioned table
                for (TableId partitionedTable : partitionedTableIds) {
                    Set<TableId> childPartitions = findChildPartitions(partitionedTable);
                    LOG.info(
                            "Found {} child partitions for table {}: {}",
                            childPartitions.size(),
                            partitionedTable,
                            childPartitions.stream()
                                    .map(TableId::toString)
                                    .collect(Collectors.joining(", ")));
                    finalTables.addAll(childPartitions);
                }
            }

            return finalTables;
        }

        private Set<TableId> findChildPartitions(TableId partitionedTable) throws SQLException {
            Set<TableId> childPartitions = new LinkedHashSet<>();

            // Use PostgresVersionStrategy for consistent query management
            PostgresVersionStrategy strategy = PostgresVersionStrategy.forConnection(connection);
            String sql =
                    PostgresSqlQueries.getQuery(
                            PostgresSqlQueries.QueryType.FIND_CHILD_PARTITIONS_ALT, strategy);

            try (Connection conn = connection.connection();
                    PreparedStatement stmt = conn.prepareStatement(sql)) {

                stmt.setString(1, partitionedTable.schema());
                stmt.setString(2, partitionedTable.table());

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String schema = rs.getString("schema_name");
                        String table = rs.getString("table_name");
                        childPartitions.add(new TableId(null, schema, table));
                    }
                }
            }

            return childPartitions;
        }

        @Override
        public String getDescription() {
            return "Child partition expansion (PostgreSQL 10-12)";
        }
    }

    /** Strategy for PostgreSQL versions that don't support declarative partitioning. */
    private static class NoPartitionSupportStrategy extends PartitionHandlingStrategy {

        @Override
        public Set<TableId> processPartitionedTables(Set<TableInfo> originalTables) {
            LOG.info("PostgreSQL version < 10 detected. No declarative partitioning support.");
            return originalTables.stream()
                    .map(TableInfo::getTableId)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        @Override
        public String getDescription() {
            return "No partition support (PostgreSQL < 10)";
        }
    }
}
