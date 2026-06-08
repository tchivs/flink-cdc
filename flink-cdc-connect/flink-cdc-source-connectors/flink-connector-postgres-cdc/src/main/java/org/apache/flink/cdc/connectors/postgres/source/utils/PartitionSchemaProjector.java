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
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Computes routed schema fingerprints and validates child schema compatibility. */
public final class PartitionSchemaProjector {

    private PartitionSchemaProjector() {}

    public static int fingerprint(Table table) {
        int hash = 17;
        for (Column column : sortedColumns(table)) {
            hash = 31 * hash + column.name().hashCode();
            hash = 31 * hash + column.typeName().hashCode();
            hash = 31 * hash + column.length();
            hash = 31 * hash + column.scale().orElse(0);
            hash = 31 * hash + column.position();
            hash = 31 * hash + (column.isOptional() ? 1 : 0);
        }
        hash = 31 * hash + table.primaryKeyColumnNames().hashCode();
        return hash;
    }

    public static void validateCompatibleChildren(
            TableId parent, Collection<TableChange> children) {
        validateHasChildren(parent, children);
        Table baseline = children.iterator().next().getTable();
        for (TableChange childChange : children) {
            Table child = childChange.getTable();
            validateCompatible(parent, baseline, child);
        }
    }

    public static Table synthesizeFromChildren(TableId parent, Collection<TableChange> children) {
        validateCompatibleChildren(parent, children);
        Table baseline = children.iterator().next().getTable();
        TableEditor editor = baseline.edit().tableId(parent);
        return editor.create();
    }

    private static void validateHasChildren(TableId parent, Collection<TableChange> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot synthesize partition schema for parent %s without child schemas",
                            parent));
        }
    }

    public static void validateCompatible(TableId parent, Table expected, Table actual) {
        if (fingerprint(expected) == fingerprint(actual)) {
            return;
        }
        List<String> expectedColumns = columnDescriptors(expected);
        List<String> actualColumns = columnDescriptors(actual);
        if (!Objects.equals(expectedColumns, actualColumns)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Incompatible partition schema for parent %s: child %s columns %s differ from child %s columns %s",
                            parent, actual.id(), actualColumns, expected.id(), expectedColumns));
        }
        if (!Objects.equals(expected.primaryKeyColumnNames(), actual.primaryKeyColumnNames())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Incompatible partition primary key for parent %s: child %s primary key %s differs from child %s primary key %s",
                            parent,
                            actual.id(),
                            actual.primaryKeyColumnNames(),
                            expected.id(),
                            expected.primaryKeyColumnNames()));
        }
    }

    private static List<String> columnDescriptors(Table table) {
        return sortedColumns(table).stream()
                .map(
                        column ->
                                column.name()
                                        + ":"
                                        + column.typeName()
                                        + ":"
                                        + column.length()
                                        + ":"
                                        + column.scale().orElse(0)
                                        + ":"
                                        + column.position()
                                        + ":"
                                        + column.isOptional())
                .collect(Collectors.toList());
    }

    private static List<Column> sortedColumns(Table table) {
        List<Column> columns = new ArrayList<>(table.columns());
        columns.sort(Comparator.comparingInt(Column::position));
        return columns;
    }
}
