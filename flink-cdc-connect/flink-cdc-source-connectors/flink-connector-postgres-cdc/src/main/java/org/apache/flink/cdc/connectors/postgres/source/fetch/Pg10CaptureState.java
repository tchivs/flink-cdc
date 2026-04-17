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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable capture state for PG10 partitioned table CDC.
 *
 * <p>Contains the routing mappings that determine how child partition events are routed to parent
 * table IDs. This class is intentionally immutable - any routing state change requires creating a
 * new instance rather than modifying the existing one.
 */
public final class Pg10CaptureState {
    private final Map<TableId, TableId> childToParentMapping;
    private final Map<TableId, List<TableId>> parentToChildrenMapping;

    private Pg10CaptureState(
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        this.childToParentMapping =
                Collections.unmodifiableMap(new LinkedHashMap<>(childToParentMapping));
        this.parentToChildrenMapping =
                Collections.unmodifiableMap(deepUnmodifiableCopy(parentToChildrenMapping));
    }

    public static Pg10CaptureState of(
            Map<TableId, TableId> childToParentMapping,
            Map<TableId, List<TableId>> parentToChildrenMapping) {
        return new Pg10CaptureState(childToParentMapping, parentToChildrenMapping);
    }

    public Map<TableId, TableId> getChildToParentMapping() {
        return childToParentMapping;
    }

    public Map<TableId, List<TableId>> getParentToChildrenMapping() {
        return parentToChildrenMapping;
    }

    private static Map<TableId, List<TableId>> deepUnmodifiableCopy(
            Map<TableId, List<TableId>> source) {
        Map<TableId, List<TableId>> copy = new LinkedHashMap<>();
        for (Map.Entry<TableId, List<TableId>> entry : source.entrySet()) {
            copy.put(
                    entry.getKey(),
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        return copy;
    }
}
