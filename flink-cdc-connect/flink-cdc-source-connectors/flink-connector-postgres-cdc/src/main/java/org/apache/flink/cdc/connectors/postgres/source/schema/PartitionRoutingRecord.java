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

package org.apache.flink.cdc.connectors.postgres.source.schema;

import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresPartitionRouting;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Carries the partition routing in force from its position of the stream on, so that the record
 * emitter routes the records after it exactly as the stream did. It is never emitted downstream.
 */
public class PartitionRoutingRecord extends SourceRecord {

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct().name("io.debezium.connector.postgres-cdc.RoutingKey").build();

    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct().name("io.debezium.connector.postgres-cdc.RoutingValue").build();

    private final PostgresPartitionRouting routing;

    public PartitionRoutingRecord(PostgresPartitionRouting routing) {
        super(
                null,
                null,
                null,
                KEY_SCHEMA,
                new Struct(KEY_SCHEMA),
                VALUE_SCHEMA,
                new Struct(VALUE_SCHEMA));
        this.routing = routing;
    }

    public PostgresPartitionRouting getRouting() {
        return routing;
    }

    @Override
    public String toString() {
        return "PartitionRoutingRecord{" + routing.routedPartitions() + '}';
    }
}
