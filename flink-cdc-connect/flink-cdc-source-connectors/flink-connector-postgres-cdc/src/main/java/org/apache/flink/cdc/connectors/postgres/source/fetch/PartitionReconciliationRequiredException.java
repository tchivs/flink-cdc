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

/** Signals that WAL processing must stop immediately for partition reconciliation. */
public class PartitionReconciliationRequiredException extends RuntimeException {

    private final TableId tableId;

    public PartitionReconciliationRequiredException(TableId tableId) {
        super("Partition reconciliation required for relation " + tableId);
        this.tableId = tableId;
    }

    public TableId getTableId() {
        return tableId;
    }
}
