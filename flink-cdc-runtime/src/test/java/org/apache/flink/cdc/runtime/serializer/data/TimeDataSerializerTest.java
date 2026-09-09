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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.binary.BinaryArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryMapData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.IntSerializer;
import org.apache.flink.cdc.runtime.serializer.NullableSerializerWrapper;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryArrayWriter;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.time.LocalTime;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeDataSerializer}. */
class TimeDataSerializerTest extends SerializerTestBase<TimeData> {
    private static final String LEGACY_SERIALIZER_BASE64 =
            "rO0ABXNyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuZGF0YS5UaW1lRGF0YVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhyAD9vcmcuYXBhY2hlLmZsaW5rLmNkYy5ydW50aW1lLnNlcmlhbGl6ZXIuVHlwZVNlcmlhbGl6ZXJTaW5nbGV0b255qYeqxy53RQIAAHhyADRvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVTZXJpYWxpemVyAAAAAAAAAAECAAB4cA==";

    @Override
    protected TypeSerializer<TimeData> createSerializer() {
        return TimeDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return 4;
    }

    @Override
    protected Class<TimeData> getTypeClass() {
        return TimeData.class;
    }

    @Override
    protected TimeData[] getTestData() {
        return new TimeData[] {
            TimeData.fromSecondOfDay(1024),
            TimeData.fromSecondOfDay(2048),
            TimeData.fromSecondOfDay(4096),
            TimeData.fromMillisOfDay(10240),
            TimeData.fromMillisOfDay(20480),
            TimeData.fromMillisOfDay(40960),
            TimeData.fromNanoOfDay(102_000_000),
            TimeData.fromNanoOfDay(204_000_000),
            TimeData.fromNanoOfDay(409_000_000),
            TimeData.fromIsoLocalTimeString("14:28:25"),
            TimeData.fromIsoLocalTimeString("01:23:45"),
            TimeData.fromIsoLocalTimeString("23:59:59"),
            TimeData.fromLocalTime(LocalTime.MIDNIGHT),
            TimeData.fromLocalTime(LocalTime.NOON)
        };
    }

    @Test
    void preservesMicrosecondsWithTimeSixSerializer() throws Exception {
        TimeDataSerializer serializer = new TimeDataSerializer(6);
        DataOutputSerializer output = new DataOutputSerializer(serializer.getLength());
        serializer.serialize(TimeData.fromMicroOfDay(14_706_123_456L), output);

        TimeData restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(restored.toMicroOfDay()).isEqualTo(14_706_123_456L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void migratesLegacyTimeSerializerNestedInNullableArraysAndMaps() throws Exception {
        TimeDataSerializer legacySerializer = readLegacySerializer();
        NullableSerializerWrapper<TimeData> legacyNullable =
                new NullableSerializerWrapper<>(legacySerializer);

        NullableSerializerWrapper.NullableSerializerWrapperSnapshot<TimeData> nullableSnapshot =
                (NullableSerializerWrapper.NullableSerializerWrapperSnapshot<TimeData>)
                        legacyNullable.snapshotConfiguration();
        assertThat(
                        nullableSnapshot
                                .resolveSchemaCompatibility(
                                        new NullableSerializerWrapper<>(new TimeDataSerializer(6)))
                                .isCompatibleAfterMigration())
                .isTrue();

        ArrayDataSerializer.ArrayDataSerializerSnapshot arraySnapshot =
                new ArrayDataSerializer.ArrayDataSerializerSnapshot(
                        DataTypes.TIME(6), legacyNullable);
        assertThat(
                        arraySnapshot
                                .resolveSchemaCompatibility(
                                        new ArrayDataSerializer(DataTypes.TIME(6)))
                                .isCompatibleAfterMigration())
                .isTrue();

        MapDataSerializer.MapDataSerializerSnapshot mapSnapshot =
                new MapDataSerializer.MapDataSerializerSnapshot(
                        DataTypes.INT(),
                        DataTypes.TIME(6),
                        IntSerializer.INSTANCE,
                        legacySerializer);
        assertThat(
                        mapSnapshot
                                .resolveSchemaCompatibility(
                                        new MapDataSerializer(DataTypes.INT(), DataTypes.TIME(6)))
                                .isCompatibleAfterMigration())
                .isTrue();
    }

    @Test
    void readsLegacyTimeArrayLayoutDirectlyAndInsideMaps() {
        BinaryArrayData keys = new BinaryArrayData();
        BinaryArrayWriter keyWriter = new BinaryArrayWriter(keys, 2, Integer.BYTES);
        keyWriter.writeInt(0, 1);
        keyWriter.writeInt(1, 2);
        keyWriter.complete();

        BinaryArrayData legacyTimes = new BinaryArrayData();
        BinaryArrayWriter timeWriter = new BinaryArrayWriter(legacyTimes, 2, Integer.BYTES);
        timeWriter.writeInt(0, 14_706_123);
        timeWriter.writeInt(1, 86_399_999);
        timeWriter.complete();

        assertThat(legacyTimes.getTime(0, 6).toMicroOfDay()).isEqualTo(14_706_123_000L);
        assertThat(legacyTimes.getTime(1, 6).toMicroOfDay()).isEqualTo(86_399_999_000L);

        BinaryArrayData singleLegacyTime = new BinaryArrayData();
        BinaryArrayWriter singleTimeWriter =
                new BinaryArrayWriter(singleLegacyTime, 1, Integer.BYTES);
        singleTimeWriter.writeInt(0, 14_706_123);
        singleTimeWriter.complete();
        assertThat(singleLegacyTime.getTime(0, 6).toMicroOfDay()).isEqualTo(14_706_123_000L);

        BinaryArrayData preciseTime =
                new ArrayDataSerializer(DataTypes.TIME(6))
                        .toBinaryArray(
                                new GenericArrayData(
                                        new Object[] {TimeData.fromMicroOfDay(14_706_123_456L)}));
        assertThat(preciseTime.getTime(0, 6).toMicroOfDay()).isEqualTo(14_706_123_456L);

        BinaryMapData legacyMap = BinaryMapData.valueOf(keys, legacyTimes);
        assertThat(legacyMap.valueArray().getTime(1, 6).toMicroOfDay()).isEqualTo(86_399_999_000L);
    }

    private static TimeDataSerializer readLegacySerializer() throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(
                                Base64.getDecoder().decode(LEGACY_SERIALIZER_BASE64)))) {
            return (TimeDataSerializer) input.readObject();
        }
    }
}
