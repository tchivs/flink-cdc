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

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Postgres connector config view whose table filter also includes known child partitions.
 *
 * <p>Debezium stores its table filter inside several runtime objects during construction. This
 * wrapper makes schema handling, dispatcher filtering, and filtered publication membership all see
 * the same partition-aware filter without changing Debezium's shared selector semantics.
 */
public class PartitionAwarePostgresConnectorConfig extends PostgresConnectorConfig {

    private final RelationalTableFilters tableFilters;

    private PartitionAwarePostgresConnectorConfig(
            Configuration config, RelationalTableFilters tableFilters) {
        super(config);
        this.tableFilters = Objects.requireNonNull(tableFilters, "tableFilters must not be null");
    }

    public static PostgresConnectorConfig wrap(
            PostgresConnectorConfig config, Supplier<PartitionRoutingState> routingStateSupplier) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(routingStateSupplier, "routingStateSupplier must not be null");

        RelationalTableFilters originalFilters = config.getTableFilters();
        Tables.TableFilter originalFilter = originalFilters.dataCollectionFilter();
        PartitionAwareTableFilter partitionAwareFilter =
                originalFilter instanceof PartitionAwareTableFilter
                        ? (PartitionAwareTableFilter) originalFilter
                        : new PartitionAwareTableFilter(originalFilter, routingStateSupplier);
        return new PartitionAwarePostgresConnectorConfig(
                config.getConfig(),
                new PartitionAwareRelationalTableFilters(originalFilters, partitionAwareFilter));
    }

    @Override
    public RelationalTableFilters getTableFilters() {
        return tableFilters;
    }

    /** Resolves captured tables to safe publication members for filtered publication updates. */
    public interface PublicationMemberResolver {
        Set<TableId> resolvePublicationMembers(Collection<TableId> capturedTables);
    }

    private static class PartitionAwareRelationalTableFilters extends RelationalTableFilters
            implements PublicationMemberResolver {

        private static final Selectors.TableIdToStringMapper TABLE_ID_MAPPER = TableId::toString;

        private final RelationalTableFilters delegate;
        private final Tables.TableFilter partitionAwareFilter;

        private PartitionAwareRelationalTableFilters(
                RelationalTableFilters delegate, Tables.TableFilter partitionAwareFilter) {
            super(Configuration.empty(), Tables.TableFilter.includeAll(), TABLE_ID_MAPPER);
            this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
            this.partitionAwareFilter =
                    Objects.requireNonNull(
                            partitionAwareFilter, "partitionAwareFilter must not be null");
        }

        @Override
        public Tables.TableFilter dataCollectionFilter() {
            return partitionAwareFilter;
        }

        public Set<TableId> resolvePublicationMembers(Collection<TableId> capturedTables) {
            return ((PartitionAwareTableFilter) partitionAwareFilter)
                    .resolvePublicationMembers(capturedTables);
        }

        @Override
        public Tables.TableFilter eligibleDataCollectionFilter() {
            return delegate.eligibleDataCollectionFilter();
        }

        @Override
        public Tables.TableFilter eligibleForSchemaDataCollectionFilter() {
            return delegate.eligibleForSchemaDataCollectionFilter();
        }

        @Override
        public Predicate<String> databaseFilter() {
            return delegate.databaseFilter();
        }

        @Override
        public String getExcludeColumns() {
            return delegate.getExcludeColumns();
        }
    }
}
