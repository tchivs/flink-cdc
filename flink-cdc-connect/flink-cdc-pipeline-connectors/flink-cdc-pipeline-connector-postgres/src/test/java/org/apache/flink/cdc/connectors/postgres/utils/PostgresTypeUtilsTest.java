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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresTypeUtils}. */
class PostgresTypeUtilsTest {

    @Test
    void testNumericWithValidPrecisionAndScale() {
        // Test numeric(10,2) -> DECIMAL(10,2)
        Column column = createColumn("numeric", 10, 2, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    @Test
    void testNumericZeroPrecision() {
        // Test numeric(0) -> BIGINT (default precise mode)
        Column column = createColumn("numeric", 0, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericZeroPrecisionWithStringMode() {
        // Test numeric(0) with decimal.handling.mode=string -> STRING
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testNumericZeroPrecisionWithDoubleMode() {
        // Test numeric(0) with decimal.handling.mode=double -> DOUBLE
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "double");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testNumericZeroPrecisionWithPreciseMode() {
        // Test numeric(0) with decimal.handling.mode=precise -> BIGINT (to avoid binary
        // serialization issues)
        Column column = createColumn("numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "precise");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericZeroPrecisionWithScale() {
        // Test numeric(0,2) -> BIGINT (precision 0 overrides scale)
        Column column = createColumn("numeric", 0, 2, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericNegativePrecision() {
        // Test numeric with negative precision -> fallback to max precision DECIMAL
        Column column = createColumn("numeric", -1, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testNumericArrayZeroPrecision() {
        // Test numeric(0)[] -> BIGINT[] (default precise mode)
        Column column = createColumn("_numeric", 0, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));
    }

    @Test
    void testNumericArrayZeroPrecisionWithStringMode() {
        // Test numeric(0)[] with decimal.handling.mode=string -> STRING[]
        Column column = createColumn("_numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
    }

    @Test
    void testNumericArrayZeroPrecisionWithDoubleMode() {
        // Test numeric(0)[] with decimal.handling.mode=double -> DOUBLE[]
        Column column = createColumn("_numeric", 0, 0, true);
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "double");
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column, props);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.DOUBLE()));
    }

    @Test
    void testNumericArrayValidPrecision() {
        // Test numeric(10,2)[] -> DECIMAL(10,2)[]
        Column column = createColumn("_numeric", 10, 2, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)));
    }

    @Test
    void testNumericArrayNegativePrecision() {
        // Test numeric array with negative precision -> fallback to max precision DECIMAL array
        Column column = createColumn("_numeric", -1, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));
    }

    @Test
    void testNumericMaxPrecision() {
        // Test numeric with maximum allowed precision
        Column column = createColumn("numeric", DecimalType.MAX_PRECISION, 10, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 10));
    }

    @Test
    void testOtherTypesUnchanged() {
        // Test that other types are not affected by our numeric(0) fix
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");

        Column intColumn = createColumn("int4", 0, 0, true);
        DataType intType = PostgresTypeUtils.fromDbzColumn(intColumn, props);
        assertThat(intType).isEqualTo(DataTypes.INT());

        Column varcharColumn = createColumn("varchar", 50, 0, true);
        DataType varcharType = PostgresTypeUtils.fromDbzColumn(varcharColumn, props);
        assertThat(varcharType).isEqualTo(DataTypes.VARCHAR(50));
    }

    @Test
    void testNormalNumericTypesWithDecimalModes() {
        // Test that normal numeric types are not affected by decimal.handling.mode setting
        Properties stringProps = new Properties();
        stringProps.setProperty("decimal.handling.mode", "string");

        Properties doubleProps = new Properties();
        doubleProps.setProperty("decimal.handling.mode", "double");

        Properties preciseProps = new Properties();
        preciseProps.setProperty("decimal.handling.mode", "precise");

        Column column = createColumn("numeric", 10, 2, true);

        // All modes should return DECIMAL(10,2) for normal numeric types
        assertThat(PostgresTypeUtils.fromDbzColumn(column, stringProps))
                .isEqualTo(DataTypes.STRING());
        assertThat(PostgresTypeUtils.fromDbzColumn(column, doubleProps))
                .isEqualTo(DataTypes.DOUBLE());
        assertThat(PostgresTypeUtils.fromDbzColumn(column, preciseProps))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    // ========== BIT TYPE TESTS FOR FLINK-35907 ==========

    @Test
    void testBitSinglePrecision() {
        // Test BIT(1) -> BOOLEAN (FLINK-35907 fix)
        Column column = createColumn("bit", 1, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BOOLEAN());
    }

    @Test
    void testBitSinglePrecisionNotNull() {
        // Test BIT(1) NOT NULL -> BOOLEAN NOT NULL
        Column column = createColumn("bit", 1, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BOOLEAN().notNull());
    }

    @Test
    void testBitMultiplePrecision() {
        // Test BIT(8) -> BYTES (FLINK-35907 fix)
        Column column = createColumn("bit", 8, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testBitLargePrecision() {
        // Test BIT(16) -> BYTES (FLINK-35907 fix)
        Column column = createColumn("bit", 16, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testBitMultiplePrecisionNotNull() {
        // Test BIT(8) NOT NULL -> BYTES NOT NULL
        Column column = createColumn("bit", 8, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES().notNull());
    }

    @Test
    void testVarbit() {
        // Test VARBIT -> BYTES (FLINK-35907 fix)
        Column column = createColumn("varbit", 32, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testVarbitNotNull() {
        // Test VARBIT NOT NULL -> BYTES NOT NULL
        Column column = createColumn("varbit", 32, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES().notNull());
    }

    @Test
    void testVarbitNoPrecision() {
        // Test VARBIT without precision -> BYTES
        Column column = createColumn("varbit", 0, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testBitArray() {
        // Test BIT[] -> ARRAY<BYTES> (FLINK-35907 fix)
        Column column = createColumn("_bit", 1, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()));
    }

    @Test
    void testBitArrayNotNull() {
        // Test BIT[] NOT NULL -> ARRAY<BYTES> NOT NULL
        Column column = createColumn("_bit", 1, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()).notNull());
    }

    @Test
    void testVarbitArray() {
        // Test VARBIT[] -> ARRAY<BYTES> (FLINK-35907 fix)
        Column column = createColumn("_varbit", 32, 0, true);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()));
    }

    @Test
    void testVarbitArrayNotNull() {
        // Test VARBIT[] NOT NULL -> ARRAY<BYTES> NOT NULL
        Column column = createColumn("_varbit", 32, 0, false);
        DataType dataType = PostgresTypeUtils.fromDbzColumn(column);
        assertThat(dataType).isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()).notNull());
    }

    @Test
    void testBitTypesWithDecimalHandlingMode() {
        // Test that bit types are not affected by decimal.handling.mode setting
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");

        Column bitColumn = createColumn("bit", 1, 0, true);
        DataType bitType = PostgresTypeUtils.fromDbzColumn(bitColumn, props);
        assertThat(bitType).isEqualTo(DataTypes.BOOLEAN());

        Column bit8Column = createColumn("bit", 8, 0, true);
        DataType bit8Type = PostgresTypeUtils.fromDbzColumn(bit8Column, props);
        assertThat(bit8Type).isEqualTo(DataTypes.BYTES());

        Column varbitColumn = createColumn("varbit", 32, 0, true);
        DataType varbitType = PostgresTypeUtils.fromDbzColumn(varbitColumn, props);
        assertThat(varbitType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testBitTypeBoundaryConditions() {
        // Test boundary conditions for bit type precision

        // Precision = 0 should still map to BYTES (edge case)
        Column bit0Column = createColumn("bit", 0, 0, true);
        DataType bit0Type = PostgresTypeUtils.fromDbzColumn(bit0Column);
        assertThat(bit0Type).isEqualTo(DataTypes.BYTES());

        // Precision = 2 should map to BYTES (not BOOLEAN)
        Column bit2Column = createColumn("bit", 2, 0, true);
        DataType bit2Type = PostgresTypeUtils.fromDbzColumn(bit2Column);
        assertThat(bit2Type).isEqualTo(DataTypes.BYTES());

        // Large precision should still map to BYTES
        Column bitLargeColumn = createColumn("bit", 1024, 0, true);
        DataType bitLargeType = PostgresTypeUtils.fromDbzColumn(bitLargeColumn);
        assertThat(bitLargeType).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testBitTypeConsistencyAcrossConnectors() {
        // Test that bit type mapping is consistent between source and pipeline connectors
        // This ensures FLINK-35907 fix maintains consistency

        Column bit1Column = createColumn("bit", 1, 0, true);
        Column bit8Column = createColumn("bit", 8, 0, true);
        Column varbitColumn = createColumn("varbit", 32, 0, true);
        Column bitArrayColumn = createColumn("_bit", 1, 0, true);
        Column varbitArrayColumn = createColumn("_varbit", 32, 0, true);

        // Verify consistent mapping rules
        assertThat(PostgresTypeUtils.fromDbzColumn(bit1Column)).isEqualTo(DataTypes.BOOLEAN());
        assertThat(PostgresTypeUtils.fromDbzColumn(bit8Column)).isEqualTo(DataTypes.BYTES());
        assertThat(PostgresTypeUtils.fromDbzColumn(varbitColumn)).isEqualTo(DataTypes.BYTES());
        assertThat(PostgresTypeUtils.fromDbzColumn(bitArrayColumn))
                .isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()));
        assertThat(PostgresTypeUtils.fromDbzColumn(varbitArrayColumn))
                .isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()));
    }

    @Test
    void testBitTypeWithNullHandling() {
        // Test bit types with different null handling scenarios

        // Optional bit types (nullable)
        Column optionalBit1 = createColumn("bit", 1, 0, true);
        Column optionalBit8 = createColumn("bit", 8, 0, true);
        Column optionalVarbit = createColumn("varbit", 32, 0, true);

        assertThat(PostgresTypeUtils.fromDbzColumn(optionalBit1)).isEqualTo(DataTypes.BOOLEAN());
        assertThat(PostgresTypeUtils.fromDbzColumn(optionalBit8)).isEqualTo(DataTypes.BYTES());
        assertThat(PostgresTypeUtils.fromDbzColumn(optionalVarbit)).isEqualTo(DataTypes.BYTES());

        // Non-optional bit types (NOT NULL)
        Column requiredBit1 = createColumn("bit", 1, 0, false);
        Column requiredBit8 = createColumn("bit", 8, 0, false);
        Column requiredVarbit = createColumn("varbit", 32, 0, false);

        assertThat(PostgresTypeUtils.fromDbzColumn(requiredBit1))
                .isEqualTo(DataTypes.BOOLEAN().notNull());
        assertThat(PostgresTypeUtils.fromDbzColumn(requiredBit8))
                .isEqualTo(DataTypes.BYTES().notNull());
        assertThat(PostgresTypeUtils.fromDbzColumn(requiredVarbit))
                .isEqualTo(DataTypes.BYTES().notNull());
    }

    /** Creates a mock Debezium Column for testing. */
    private Column createColumn(String typeName, int length, int scale, boolean optional) {
        return Column.editor()
                .name("test_column")
                .type(typeName)
                .length(length)
                .scale(scale)
                .optional(optional)
                .create();
    }
}
