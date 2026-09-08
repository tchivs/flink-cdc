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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests stable and lossless representations of PostgreSQL NUMERIC values. */
class PostgresVariableScaleDecimalTest {

    @Test
    void snapshotAndWalScalarsUseOneStableSchemaAndPreserveEveryDigit() throws Exception {
        Schema decimalSchema = VariableScaleDecimal.optionalSchema();
        Schema rowSchema = rowSchema("capture.public.items.Value", decimalSchema);
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        List<BigDecimal> values =
                Arrays.asList(
                        null,
                        BigDecimal.ZERO,
                        new BigDecimal("-123.4500"),
                        new BigDecimal("116.397128000000000001"),
                        new BigDecimal("12345678901234567890123456789012345678901234567890"));

        assertThat(new PostgresSchemaDataTypeInference().infer(null, decimalSchema))
                .isEqualTo(DataTypes.STRING());
        for (String operation : Arrays.asList("r", "c")) {
            for (BigDecimal value : values) {
                Struct encoded =
                        value == null
                                ? null
                                : VariableScaleDecimal.fromLogical(decimalSchema, value);
                DataChangeEvent event =
                        deserializer
                                .deserializeDataChangeRecord(record(rowSchema, encoded, operation))
                                .get(0);
                RecordData after = event.after();

                if (value == null) {
                    assertThat(after.isNullAt(0)).isTrue();
                } else {
                    assertThat(after.getString(0).toString()).isEqualTo(value.toPlainString());
                }
            }
        }
    }

    @Test
    void zeroAndLaterHighPrecisionValueDoNotChangeTheFieldType() {
        Schema schema = VariableScaleDecimal.schema();
        Struct zero = VariableScaleDecimal.fromLogical(schema, BigDecimal.ZERO);
        Struct highPrecision =
                VariableScaleDecimal.fromLogical(
                        schema, new BigDecimal("-98765432109876543210.000000000000000001"));
        PostgresSchemaDataTypeInference inference = new PostgresSchemaDataTypeInference();

        assertThat(inference.infer(zero, schema)).isEqualTo(DataTypes.STRING().notNull());
        assertThat(inference.infer(highPrecision, schema)).isEqualTo(DataTypes.STRING().notNull());
    }

    @Test
    void explicitSupportedPrecisionDecimalKeepsDecimalSemantics() throws Exception {
        Schema decimalSchema =
                Decimal.builder(2).parameter("connect.decimal.precision", "10").optional().build();
        Schema rowSchema = rowSchema("capture.public.fixed_numeric.Value", decimalSchema);
        BigDecimal value = new BigDecimal("-123.45");
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        DataChangeEvent event =
                deserializer.deserializeDataChangeRecord(record(rowSchema, value, "r")).get(0);

        assertThat(event.after().getDecimal(0, 10, 2).toBigDecimal()).isEqualTo(value);
        assertThat(new PostgresSchemaDataTypeInference().infer(value, decimalSchema))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    @Test
    void explicitMaximumScaleDecimalKeepsDecimalSemantics() throws Exception {
        Schema decimalSchema =
                Decimal.builder(38).parameter("connect.decimal.precision", "38").optional().build();
        Schema rowSchema = rowSchema("capture.public.maximum_scale.Value", decimalSchema);
        BigDecimal value = new BigDecimal("0.00000000000000000000000000000000000001");
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        DataChangeEvent event =
                deserializer.deserializeDataChangeRecord(record(rowSchema, value, "c")).get(0);

        assertThat(event.after().getDecimal(0, 38, 38).toBigDecimal()).isEqualTo(value);
        assertThat(new PostgresSchemaDataTypeInference().infer(value, decimalSchema))
                .isEqualTo(DataTypes.DECIMAL(38, 38));
    }

    @Test
    void highPrecisionFixedDecimalUsesPlainStringEncoding() throws Exception {
        Schema decimalSchema =
                Decimal.builder(40).parameter("connect.decimal.precision", "50").optional().build();
        Schema rowSchema = rowSchema("capture.public.high_precision.Value", decimalSchema);
        BigDecimal value = new BigDecimal("1E-40");
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        assertThat(new PostgresSchemaDataTypeInference().infer(value, decimalSchema))
                .isEqualTo(DataTypes.STRING());
        DataChangeEvent event =
                deserializer.deserializeDataChangeRecord(record(rowSchema, value, "c")).get(0);

        assertThat(event.after().getString(0).toString()).isEqualTo(value.toPlainString());
        assertThat(
                        deserializer.convertToString(
                                Decimal.fromLogical(decimalSchema, value), decimalSchema))
                .isEqualTo(BinaryStringData.fromString(value.toPlainString()));
    }

    @Test
    void variableScaleConverterUsesPlainNotation() {
        Schema schema = VariableScaleDecimal.schema();
        BigDecimal value = new BigDecimal("0.00000000000000000000000000000000000001");
        Struct encoded = VariableScaleDecimal.fromLogical(schema, value);
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        assertThat(deserializer.convertToString(encoded, schema))
                .isEqualTo(BinaryStringData.fromString(value.toPlainString()));
    }

    @Test
    void variableScaleSpecialValuesUseCanonicalText() {
        Schema schema = VariableScaleDecimal.schema();
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        List<SpecialValueDecimal> values =
                Arrays.asList(
                        SpecialValueDecimal.NOT_A_NUMBER,
                        SpecialValueDecimal.POSITIVE_INF,
                        SpecialValueDecimal.NEGATIVE_INF);
        List<String> expected =
                Arrays.asList("NAN", "POSITIVE_INFINITY", "NEGATIVE_INFINITY");

        for (int i = 0; i < values.size(); i++) {
            Struct encoded = VariableScaleDecimal.fromLogical(schema, values.get(i));
            assertThat(deserializer.convertToString(encoded, schema))
                    .isEqualTo(BinaryStringData.fromString(expected.get(i)));
        }
    }

    @Test
    void snapshotAndWalArraysKeepOneStringElementType() throws Exception {
        Schema decimalSchema = VariableScaleDecimal.optionalSchema();
        Schema arraySchema = SchemaBuilder.array(decimalSchema).optional().build();
        Schema rowSchema = rowSchema("capture.public.numeric_array.Value", arraySchema);
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        BigDecimal highPrecision = new BigDecimal("-98765432109876543210.000000000000000001");
        List<Struct> encoded =
                Arrays.asList(
                        null,
                        VariableScaleDecimal.fromLogical(decimalSchema, BigDecimal.ZERO),
                        VariableScaleDecimal.fromLogical(decimalSchema, highPrecision));

        assertThat(new PostgresSchemaDataTypeInference().infer(null, arraySchema))
                .isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        for (String operation : Arrays.asList("r", "c")) {
            DataChangeEvent event =
                    deserializer
                            .deserializeDataChangeRecord(record(rowSchema, encoded, operation))
                            .get(0);
            ArrayData amounts = event.after().getArray(0);

            assertThat(amounts.size()).isEqualTo(3);
            assertThat(amounts.isNullAt(0)).isTrue();
            assertThat(amounts.getString(1).toString()).isEqualTo("0");
            assertThat(amounts.getString(2).toString()).isEqualTo(highPrecision.toPlainString());
        }
    }

    @Test
    void highPrecisionFixedDecimalArraysUsePlainStringEncoding() throws Exception {
        Schema decimalSchema =
                Decimal.builder(40).parameter("connect.decimal.precision", "50").optional().build();
        Schema arraySchema = SchemaBuilder.array(decimalSchema).optional().build();
        Schema rowSchema = rowSchema("capture.public.high_precision_array.Value", arraySchema);
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);
        List<BigDecimal> values =
                Arrays.asList(
                        new BigDecimal("1E-40"),
                        new BigDecimal(
                                "1234567890.1234567890123456789012345678901234567890"));

        assertThat(new PostgresSchemaDataTypeInference().infer(null, arraySchema))
                .isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        for (String operation : Arrays.asList("r", "c")) {
            DataChangeEvent event =
                    deserializer
                            .deserializeDataChangeRecord(record(rowSchema, values, operation))
                            .get(0);
            ArrayData amounts = event.after().getArray(0);

            assertThat(amounts.getString(0).toString()).isEqualTo(values.get(0).toPlainString());
            assertThat(amounts.getString(1).toString()).isEqualTo(values.get(1).toPlainString());
        }
    }

    private static Schema rowSchema(String name, Schema amountSchema) {
        return SchemaBuilder.struct().name(name).optional().field("amount", amountSchema).build();
    }

    private static SourceRecord record(Schema rowSchema, Object amount, String operation) {
        Schema envelopeSchema =
                SchemaBuilder.struct()
                        .field("before", rowSchema)
                        .field("after", rowSchema)
                        .field("op", Schema.STRING_SCHEMA)
                        .build();
        Struct after = new Struct(rowSchema).put("amount", amount);
        Struct envelope =
                new Struct(envelopeSchema)
                        .put("before", null)
                        .put("after", after)
                        .put("op", operation);
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "capture.public.items",
                null,
                null,
                envelopeSchema,
                envelope);
    }
}
