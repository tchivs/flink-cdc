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

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A utility class for table discovery. */
public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    public static List<TableId> listTables(
            String database,
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            boolean includePartitionedTables)
            throws SQLException {

        String[] tableTypes = new String[] {"TABLE"};
        if (includePartitionedTables) {
            tableTypes = new String[] {"TABLE", "PARTITIONED TABLE"};
        }
        Set<TableId> allTableIds = jdbc.readTableNames(database, null, null, tableTypes);

        Set<TableId> capturedTables =
                allTableIds.stream()
                        .filter(t -> tableFilters.dataCollectionFilter().isIncluded(t))
                        .collect(Collectors.toSet());
        LOG.info(
                "Postgres captured tables : {} .",
                capturedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));

        return new ArrayList<>(capturedTables);
    }

    /**
     * Discovers partition mappings and determines whether partition routing should be enabled.
     *
     * <p>Routing is enabled when:
     *
     * <ul>
     *   <li>{@code includePartitionedTables} is true, AND
     *   <li>At least one parent table has child partitions in pg_inherits
     * </ul>
     *
     * @param parentTableIds the parent tables discovered by {@link #listTables}
     * @param jdbc the JDBC connection
     * @param includePartitionedTables whether the user opted into partitioned table support
     * @param publicationName the publication name (for validation), may be null
     * @return a fully-initialized {@link PartitionCaptureState}
     */
    public static PartitionCaptureState discoverPartitionState(
            List<TableId> parentTableIds,
            JdbcConnection jdbc,
            boolean includePartitionedTables,
            String publicationName)
            throws SQLException {
        if (!includePartitionedTables) {
            return PartitionCaptureState.EMPTY;
        }

        Map<TableId, List<TableId>> parentToChildren =
                PartitionMapper.discoverPartitionMappings(parentTableIds, jdbc, true);

        if (parentToChildren.isEmpty()) {
            LOG.info("No child partitions found for configured parent tables.");
            return PartitionCaptureState.EMPTY;
        }

        LOG.info(
                "Partition routing enabled: {} parents with {} total children",
                parentToChildren.size(),
                parentToChildren.values().stream().mapToInt(List::size).sum());

        return PartitionCaptureState.of(parentToChildren, true, publicationName);
    }
}
