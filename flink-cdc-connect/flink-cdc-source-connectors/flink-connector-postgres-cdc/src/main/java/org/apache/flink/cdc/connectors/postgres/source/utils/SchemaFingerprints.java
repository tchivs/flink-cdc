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

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.util.List;

/**
 * Utility for computing schema fingerprints to detect whether a table's schema has changed.
 *
 * <p>The fingerprint covers column count, column names, type names, lengths, scales, nullable
 * flags, and ordinal positions. This provides strong collision resistance while remaining
 * lightweight (no allocation, O(columns) time). Used by both the WAL schema layer and the event
 * dispatcher to skip redundant schema processing when the same table structure arrives repeatedly
 * via pgoutput Relation messages.
 *
 * <p>Note: The fingerprint is based on a polynomial hash using factor 31. While hash collisions are
 * theoretically possible, including both structural (nullable, position) and type information makes
 * collision probability negligibly small for practical DDL evolution scenarios.
 */
public final class SchemaFingerprints {

    private SchemaFingerprints() {}

    /**
     * Computes a fingerprint of the table's column structure.
     *
     * <p>Includes: column count, each column's name, typeName, length, scale, nullable flag, and
     * position. This ensures that any practical DDL change (ADD/DROP/ALTER COLUMN) produces a
     * different fingerprint.
     *
     * @param table the table whose schema to fingerprint
     * @return an integer hash representing the schema structure
     */
    public static int computeSchemaFingerprint(Table table) {
        List<Column> columns = table.columns();
        int hash = columns.size();
        for (Column col : columns) {
            hash = 31 * hash + col.name().hashCode();
            hash = 31 * hash + col.typeName().hashCode();
            hash = 31 * hash + col.length();
            hash = 31 * hash + col.scale().orElse(0);
            hash = 31 * hash + (col.isOptional() ? 1 : 0);
            hash = 31 * hash + col.position();
        }
        return hash;
    }
}
