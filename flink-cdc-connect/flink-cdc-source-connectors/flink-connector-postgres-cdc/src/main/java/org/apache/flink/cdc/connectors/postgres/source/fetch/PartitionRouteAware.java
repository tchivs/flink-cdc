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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import io.debezium.relational.TableId;

/**
 * Capability marker for Debezium {@code PostgresEventDispatcher} implementations that can pre-route
 * a child partition tableId to its parent before the change-record emitter is constructed.
 *
 * <p>Implementations must be safe to call from the WAL streaming thread ({@code
 * PostgresStreamingChangeEventSource}). They should also be idempotent and cheap: a no-op fast path
 * is required when no partition routing is configured so that the streaming path does not pay for a
 * feature it does not use.
 *
 * <p>The streaming event source calls {@link #preRouteTableId(TableId)} unconditionally and falls
 * back to the input tableId if this capability is not present on the dispatcher. This avoids
 * hard-coding the concrete {@code CDCPostgresDispatcher} type at the call site and keeps the
 * dispatcher hierarchy open for extension (Liskov-friendly).
 */
public interface PartitionRouteAware {

    /**
     * Pre-routes a child partition tableId to its parent tableId. Returns the input tableId
     * unchanged when no routing is configured or the input is not a known child.
     *
     * @param tableId the original tableId from the WAL message
     * @return the routed tableId (parent if routing is active, otherwise the original)
     */
    default TableId preRouteTableId(TableId tableId) {
        return tableId;
    }
}
