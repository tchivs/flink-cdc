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

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Table filter that lets known child partitions pass Debezium schema and dispatcher filters. */
public class PartitionAwareTableFilter implements Tables.TableFilter {

    private final Tables.TableFilter originalFilter;
    private final Supplier<PartitionRoutingState> routingStateSupplier;

    public PartitionAwareTableFilter(
            Tables.TableFilter originalFilter,
            Supplier<PartitionRoutingState> routingStateSupplier) {
        this.originalFilter =
                Objects.requireNonNull(originalFilter, "originalFilter must not be null");
        this.routingStateSupplier =
                Objects.requireNonNull(
                        routingStateSupplier, "routingStateSupplier must not be null");
    }

    @Override
    public boolean isIncluded(TableId tableId) {
        if (originalFilter.isIncluded(tableId)) {
            return true;
        }
        PartitionRoutingState routingState = routingStateSupplier.get();
        if (routingState == null) {
            throw new IllegalStateException(
                    "Partition routing state is not initialized for partition-aware table filter");
        }
        return routingState.containsChild(tableId);
    }

    public Tables.TableFilter originalFilter() {
        return originalFilter;
    }

    public Set<TableId> resolvePublicationMembers(Collection<TableId> capturedTables) {
        Set<TableId> requestedMembers = new LinkedHashSet<>();
        PartitionRoutingState routingState = routingStateSupplier.get();
        if (routingState == null) {
            throw new IllegalStateException(
                    "Partition routing state is not initialized for partition-aware publication filter");
        }
        for (TableId tableId : capturedTables) {
            if (originalFilter.isIncluded(tableId) || !routingState.containsChild(tableId)) {
                requestedMembers.add(tableId);
            }
        }
        return new LinkedHashSet<>(
                PartitionPublicationRefresher.computeDesiredMembers(
                        requestedMembers, routingState));
    }
}
