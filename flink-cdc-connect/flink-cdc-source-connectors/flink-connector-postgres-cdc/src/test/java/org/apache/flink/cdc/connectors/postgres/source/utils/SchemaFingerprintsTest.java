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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SchemaFingerprints#computeSchemaFingerprint(Table)}. */
class SchemaFingerprintsTest {

    private static final TableId TABLE_ID = new TableId(null, "public", "products");

    private static Table baseTable() {
        return tableBuilder()
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("INT4")
                                .position(1)
                                .optional(false)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("name")
                                .type("VARCHAR")
                                .length(64)
                                .position(2)
                                .optional(true)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static TableEditor tableBuilder() {
        return Table.editor().tableId(TABLE_ID);
    }

    @Test
    void identicalSchemasProduceSameFingerprint() {
        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(baseTable()))
                .isEqualTo(SchemaFingerprints.computeSchemaFingerprint(baseTable()));
    }

    @Test
    void addingColumnChangesFingerprint() {
        Table base = baseTable();
        Table modified =
                base.edit()
                        .addColumn(
                                Column.editor()
                                        .name("price")
                                        .type("NUMERIC")
                                        .length(10)
                                        .scale(2)
                                        .position(3)
                                        .optional(true)
                                        .create())
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(modified));
    }

    @Test
    void droppingColumnChangesFingerprint() {
        Table base = baseTable();
        Table dropped = base.edit().removeColumn("name").create();
        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(dropped));
    }

    @Test
    void renamingColumnChangesFingerprint() {
        Table base = baseTable();
        Column nameCol = base.columnWithName("name");
        Table renamed =
                base.edit()
                        .removeColumn("name")
                        .addColumn(
                                Column.editor()
                                        .name("nickname")
                                        .type(nameCol.typeName())
                                        .length(nameCol.length())
                                        .position(nameCol.position())
                                        .optional(nameCol.isOptional())
                                        .create())
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(renamed));
    }

    @Test
    void changingTypeChangesFingerprint() {
        Table base = baseTable();
        Table changed =
                base.edit()
                        .removeColumn("name")
                        .addColumn(
                                Column.editor()
                                        .name("name")
                                        .type("TEXT") // was VARCHAR(64)
                                        .position(2)
                                        .optional(true)
                                        .create())
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(changed));
    }

    @Test
    void changingNullableChangesFingerprint() {
        Table base = baseTable();
        Table flipped =
                base.edit()
                        .removeColumn("name")
                        .addColumn(
                                Column.editor()
                                        .name("name")
                                        .type("VARCHAR")
                                        .length(64)
                                        .position(2)
                                        .optional(false) // was true
                                        .create())
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(flipped));
    }

    @Test
    void changingPositionChangesFingerprint() {
        Table base = baseTable();
        Table reordered =
                Table.editor()
                        .tableId(TABLE_ID)
                        .addColumn(
                                Column.editor()
                                        .name("name")
                                        .type("VARCHAR")
                                        .length(64)
                                        .position(1) // swapped
                                        .optional(true)
                                        .create())
                        .addColumn(
                                Column.editor()
                                        .name("id")
                                        .type("INT4")
                                        .position(2) // swapped
                                        .optional(false)
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(reordered));
    }

    @Test
    void changingLengthChangesFingerprint() {
        Table base = baseTable();
        Table widened =
                base.edit()
                        .removeColumn("name")
                        .addColumn(
                                Column.editor()
                                        .name("name")
                                        .type("VARCHAR")
                                        .length(128) // was 64
                                        .position(2)
                                        .optional(true)
                                        .create())
                        .create();

        Assertions.assertThat(SchemaFingerprints.computeSchemaFingerprint(base))
                .isNotEqualTo(SchemaFingerprints.computeSchemaFingerprint(widened));
    }
}
