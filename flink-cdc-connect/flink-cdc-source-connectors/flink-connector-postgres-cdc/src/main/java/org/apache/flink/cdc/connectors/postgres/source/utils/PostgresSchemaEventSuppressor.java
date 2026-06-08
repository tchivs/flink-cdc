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

import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Suppresses duplicate schema events after child partition tables are routed to parents. */
public class PostgresSchemaEventSuppressor {

    private final Map<RoutedChildKey, Integer> fingerprintsByRoutedChild = new HashMap<>();

    public boolean shouldDispatch(Table originalTable, Table routedTable) {
        if (originalTable == null || routedTable == null) {
            return true;
        }
        if (originalTable.id().equals(routedTable.id())) {
            return true;
        }
        int fingerprint = PartitionSchemaProjector.fingerprint(originalTable);
        RoutedChildKey key = new RoutedChildKey(routedTable.id(), originalTable.id());
        Integer previous = fingerprintsByRoutedChild.put(key, fingerprint);
        if (previous == null) {
            return true;
        }
        return previous != fingerprint;
    }

    private static class RoutedChildKey {
        private final TableId parent;
        private final TableId child;

        private RoutedChildKey(TableId parent, TableId child) {
            this.parent = parent;
            this.child = child;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RoutedChildKey)) {
                return false;
            }
            RoutedChildKey that = (RoutedChildKey) o;
            return Objects.equals(parent, that.parent) && Objects.equals(child, that.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parent, child);
        }
    }
}
