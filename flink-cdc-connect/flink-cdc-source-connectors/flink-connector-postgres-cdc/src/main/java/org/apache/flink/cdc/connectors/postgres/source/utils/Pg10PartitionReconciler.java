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
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure catalog-diff component for PostgreSQL 10 partition reconciliation.
 *
 * <p>This class compares previous partition mappings against latest catalog state and reports only
 * what has changed. It has NO side effects - it does not mutate publications or any runtime state.
 * Publication mutation is handled separately by {@link Pg10PublicationManager}.
 */
public final class Pg10PartitionReconciler {

    /**
     * Reconciles current partition mappings against the latest catalog state.
     *
     * @param jdbc the JDBC connection for catalog queries
     * @param parentTables the list of parent TableIds to discover children for
     * @param previousParentToChildren the previously known parent→children mappings
     * @param previousChildToParent the previously known child→parent mappings
     * @return a reconcile result containing new children and next-state mappings
     * @throws SQLException if catalog queries fail
     */
    public Pg10ReconcileResult reconcile(
            JdbcConnection jdbc,
            List<TableId> parentTables,
            Map<TableId, List<TableId>> previousParentToChildren,
            Map<TableId, TableId> previousChildToParent)
            throws SQLException {
        Map<TableId, List<TableId>> latestParentToChildren =
                TableDiscoveryUtils.discoverPartitionedTableMappings(parentTables, jdbc, true);
        return reconcile(previousParentToChildren, previousChildToParent, latestParentToChildren);
    }

    public Pg10ReconcileResult reconcile(
            Map<TableId, List<TableId>> previousParentToChildren,
            Map<TableId, TableId> previousChildToParent,
            Map<TableId, List<TableId>> latestParentToChildren) {
        Map<TableId, TableId> latestChildToParent =
                Pg10PartitionMapper.buildChildToParentMapping(latestParentToChildren);
        return Pg10ReconcileResult.from(
                previousParentToChildren,
                previousChildToParent,
                latestParentToChildren,
                latestChildToParent);
    }

    /**
     * Immutable result of a PG10 partition reconciliation operation.
     *
     * <p>Contains both the diffed new children and the complete next-state mappings.
     */
    public static final class Pg10ReconcileResult {
        private final Map<TableId, List<TableId>> previousParentToChildren;
        private final Map<TableId, TableId> previousChildToParent;
        private final Map<TableId, List<TableId>> latestParentToChildren;
        private final Map<TableId, TableId> latestChildToParent;

        private Pg10ReconcileResult(
                Map<TableId, List<TableId>> previousParentToChildren,
                Map<TableId, TableId> previousChildToParent,
                Map<TableId, List<TableId>> latestParentToChildren,
                Map<TableId, TableId> latestChildToParent) {
            this.previousParentToChildren =
                    Collections.unmodifiableMap(
                            new java.util.LinkedHashMap<>(previousParentToChildren));
            this.previousChildToParent =
                    Collections.unmodifiableMap(
                            new java.util.LinkedHashMap<>(previousChildToParent));
            this.latestParentToChildren =
                    Collections.unmodifiableMap(
                            new java.util.LinkedHashMap<>(latestParentToChildren));
            this.latestChildToParent =
                    Collections.unmodifiableMap(new java.util.LinkedHashMap<>(latestChildToParent));
        }

        public static Pg10ReconcileResult from(
                Map<TableId, List<TableId>> previousParentToChildren,
                Map<TableId, TableId> previousChildToParent,
                Map<TableId, List<TableId>> latestParentToChildren,
                Map<TableId, TableId> latestChildToParent) {
            return new Pg10ReconcileResult(
                    previousParentToChildren,
                    previousChildToParent,
                    latestParentToChildren,
                    latestChildToParent);
        }

        public Map<TableId, List<TableId>> getPreviousParentToChildren() {
            return previousParentToChildren;
        }

        public Map<TableId, TableId> getPreviousChildToParent() {
            return previousChildToParent;
        }

        public Map<TableId, List<TableId>> getLatestParentToChildren() {
            return latestParentToChildren;
        }

        public Map<TableId, TableId> getLatestChildToParent() {
            return latestChildToParent;
        }

        /** Returns child partitions that exist now but didn't in the previous state. */
        public List<TableId> getNewChildren() {
            Set<TableId> oldChildren = new LinkedHashSet<>(previousChildToParent.keySet());
            List<TableId> newChildren = new ArrayList<>();
            for (TableId childTable : latestChildToParent.keySet()) {
                if (!oldChildren.contains(childTable)) {
                    newChildren.add(childTable);
                }
            }
            return newChildren;
        }

        /** Returns the child→parent mapping for the NEW children only. */
        public Map<TableId, TableId> getNewChildToParent() {
            List<TableId> newChildren = getNewChildren();
            Map<TableId, TableId> newChildToParent = new java.util.LinkedHashMap<>();
            for (TableId newChild : newChildren) {
                newChildToParent.put(newChild, latestChildToParent.get(newChild));
            }
            return newChildToParent;
        }
    }
}
