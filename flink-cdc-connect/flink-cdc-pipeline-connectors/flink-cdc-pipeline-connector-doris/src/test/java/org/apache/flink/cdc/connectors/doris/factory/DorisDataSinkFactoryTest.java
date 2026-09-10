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

package org.apache.flink.cdc.connectors.doris.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.doris.sink.DorisMetadataApplier;
import org.apache.flink.table.api.ValidationException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SCHEMA_CHANGE_ALLOWED_TYPES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_BUCKETS;

/** Tests for {@link DorisDataSinkFactory}. */
class DorisDataSinkFactoryTest {

    @Test
    void testNewOptionsAreOptional() {
        DorisMetadataApplier metadataApplier =
                (DorisMetadataApplier) createDataSink(Collections.emptyMap()).getMetadataApplier();

        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(CREATE_TABLE)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ADD_COLUMN)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ALTER_COLUMN_TYPE))
                .isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(DROP_COLUMN)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(RENAME_COLUMN)).isTrue();
    }

    @Test
    void testTableCreateBucketBounds() {
        for (int validBuckets : Arrays.asList(1, 256)) {
            Assertions.assertThatCode(
                            () ->
                                    createDataSink(
                                            Collections.singletonMap(
                                                    TABLE_CREATE_BUCKETS.key(),
                                                    Integer.toString(validBuckets))))
                    .doesNotThrowAnyException();
        }

        for (int invalidBuckets : Arrays.asList(0, 257)) {
            Assertions.assertThatThrownBy(
                            () ->
                                    createDataSink(
                                            Collections.singletonMap(
                                                    TABLE_CREATE_BUCKETS.key(),
                                                    Integer.toString(invalidBuckets))))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("must be between 1 and 256");
        }
    }

    @Test
    void testStrictSchemaChangeAllowlistParsing() {
        DorisMetadataApplier metadataApplier =
                (DorisMetadataApplier)
                        createDataSink(
                                        Collections.singletonMap(
                                                SCHEMA_CHANGE_ALLOWED_TYPES.key(), "ADD_COLUMN"))
                                .getMetadataApplier();
        metadataApplier.setAcceptedSchemaEvolutionTypes(EnumSet.allOf(SchemaChangeEventType.class));

        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(CREATE_TABLE)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ADD_COLUMN)).isTrue();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(DROP_COLUMN)).isFalse();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(RENAME_COLUMN)).isFalse();
        Assertions.assertThat(metadataApplier.acceptsSchemaEvolutionType(ALTER_COLUMN_TYPE))
                .isFalse();
    }

    @Test
    void testRejectsMalformedSchemaChangeAllowlists() {
        for (String invalidAllowlist :
                Arrays.asList("", "add_column", "ADD_COLUMN,", "ALTER_TABLE_COMMENT")) {
            Assertions.assertThatThrownBy(
                            () ->
                                    createDataSink(
                                            Collections.singletonMap(
                                                    SCHEMA_CHANGE_ALLOWED_TYPES.key(),
                                                    invalidAllowlist)))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(SCHEMA_CHANGE_ALLOWED_TYPES.key());
        }
    }

    @Test
    void testRejectsMalformedVisibleColumnConfiguration() {
        assertInvalid(options("sink.delete-mode", "VISIBLE"), "visible-delete-column");
        assertInvalid(
                options("sink.delete-mode", "VISIBLE", "sink.visible-delete-column", "unsafe-name"),
                "unsafe Doris identifier");
        assertInvalid(options("sink.delete-mode", "visible"), "PHYSICAL or VISIBLE");
    }

    @Test
    void testRejectsMalformedMetadataColumnConfiguration() {
        assertInvalid(
                options("sink.metadata-columns.source_lsn.source", "source.lsn"),
                "both '.source' and '.type'");
        assertInvalid(
                options(
                        "sink.metadata-columns.source_lsn.source",
                        "source.lsn",
                        "sink.metadata-columns.source_lsn.type",
                        "bigint"),
                "BIGINT, BOOLEAN, or STRING");
        assertInvalid(
                options(
                        "sink.metadata-columns.deleted.source",
                        "source.deleted",
                        "sink.metadata-columns.deleted.type",
                        "BOOLEAN",
                        "sink.metadata-columns.deleted.default",
                        "TRUE"),
                "canonical true or false");
        assertInvalid(
                options(
                        "sink.metadata-columns.source_lsn.source",
                        "source.lsn",
                        "sink.metadata-columns.source_lsn.type",
                        "BIGINT",
                        "sink.metadata-columns.source_lsn.default",
                        "9223372036854775808"),
                "must be a BIGINT");
        assertInvalid(
                options(
                        "sink.metadata-columns.source_lsn.source",
                        "source.lsn",
                        "sink.metadata-columns.source_lsn.type",
                        "BIGINT",
                        "sink.metadata-columns.source_lsn.default",
                        "0",
                        "table.create.properties.function_column.sequence_col",
                        "source_lsn"),
                "must not configure a default");
        assertInvalid(
                options(
                        "sink.metadata-columns.source_lsn.source",
                        "source.lsn",
                        "sink.metadata-columns.source_lsn.type",
                        "STRING",
                        "table.create.properties.function_column.sequence_col",
                        "source_lsn"),
                "must be configured as BIGINT");
    }

    @Test
    void testRejectsConfiguredColumnCollisions() {
        assertInvalid(
                options(
                        "sink.delete-mode",
                        "VISIBLE",
                        "sink.visible-delete-column",
                        "deleted",
                        "sink.metadata-columns.DELETED.source",
                        "source.deleted",
                        "sink.metadata-columns.DELETED.type",
                        "BOOLEAN"),
                "collides");
        assertInvalid(
                options(
                        "sink.metadata-columns.Meta.source",
                        "source.one",
                        "sink.metadata-columns.Meta.type",
                        "STRING",
                        "sink.metadata-columns.meta.source",
                        "source.two",
                        "sink.metadata-columns.meta.type",
                        "STRING"),
                "unique ignoring case");
    }

    private static void assertInvalid(Map<String, String> options, String message) {
        Assertions.assertThatThrownBy(() -> createDataSink(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(message);
    }

    private static Map<String, String> options(String... keyValues) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            options.put(keyValues[i], keyValues[i + 1]);
        }
        return options;
    }

    private static DataSink createDataSink(Map<String, String> additionalOptions) {
        Map<String, String> options = new HashMap<>();
        options.put("fenodes", "127.0.0.1:8030");
        options.put("username", "root");
        options.putAll(additionalOptions);
        Configuration configuration = Configuration.fromMap(options);
        Configuration pipelineConfiguration =
                Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        return new DorisDataSinkFactory()
                .createDataSink(
                        new FactoryHelper.DefaultContext(
                                configuration,
                                pipelineConfiguration,
                                Thread.currentThread().getContextClassLoader()));
    }
}
