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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataMetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;

/** Defines the supported metadata columns for {@link PostgreSQLTableSource}. */
public enum PostgreSQLReadableMetadata {
    /** Name of the table that contain the row. */
    TABLE_NAME(
            "table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.TABLE_NAME_KEY));
                }
            }),

    /** Name of the schema that contain the row. */
    SCHEMA_NAME(
            "schema_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.SCHEMA_NAME_KEY));
                }
            }),

    /** Name of the database that contain the row. */
    DATABASE_NAME(
            "database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the change stream, the value is always 0.
     */
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP_LTZ(3).notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return TimestampData.fromEpochMillis(
                            (Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY));
                }
            }),

    /**
     * It indicates the row kind of the changelog. '+I' means INSERT message, '-D' means DELETE
     * message, '-U' means UPDATE_BEFORE message and '+U' means UPDATE_AFTER message
     */
    ROW_KIND(
            "row_kind",
            DataTypes.STRING().notNull(),
            new RowDataMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(RowData rowData) {
                    return StringData.fromString(rowData.getRowKind().shortString());
                }

                @Override
                public Object read(SourceRecord record) {
                    throw new UnsupportedOperationException(
                            "Please call read(RowData rowData) method instead.");
                }
            }),

    /** Original Debezium operation code: r, c, u, or d. */
    SOURCE_OPERATION("source.op", DataTypes.STRING().notNull(), operationConverter()),

    /** Original source database name, independent of downstream table routing. */
    SOURCE_DATABASE(
            "source.database",
            DataTypes.STRING().notNull(),
            sourceFieldConverter(AbstractSourceInfo.DATABASE_NAME_KEY, false)),

    /** Original source schema name, independent of downstream table routing. */
    SOURCE_SCHEMA(
            "source.schema",
            DataTypes.STRING().notNull(),
            sourceFieldConverter(AbstractSourceInfo.SCHEMA_NAME_KEY, false)),

    /** Original source table name, independent of downstream table routing. */
    SOURCE_TABLE(
            "source.table",
            DataTypes.STRING().notNull(),
            sourceFieldConverter(AbstractSourceInfo.TABLE_NAME_KEY, false)),

    /** Unsigned decimal PostgreSQL LSN. A value of 0 on a READ record is a snapshot sentinel. */
    SOURCE_LSN("source.lsn", DataTypes.STRING(), sourceFieldConverter(SourceInfo.LSN_KEY, true)),

    /** PostgreSQL transaction identifier when supplied by the source. */
    SOURCE_TRANSACTION_ID(
            "source.tx-id", DataTypes.STRING(), sourceFieldConverter(SourceInfo.TXID_KEY, false)),

    /** Debezium's ordered source sequence JSON string. */
    SOURCE_SEQUENCE(
            "source.sequence",
            DataTypes.STRING(),
            sourceFieldConverter(AbstractSourceInfo.SEQUENCE_KEY, false)),

    /** Debezium snapshot marker: true, last, false, or incremental. */
    SOURCE_SNAPSHOT(
            "source.snapshot",
            DataTypes.STRING(),
            sourceFieldConverter(AbstractSourceInfo.SNAPSHOT_KEY, false)),

    /** Source timestamp in milliseconds since the epoch. */
    SOURCE_TIMESTAMP_MS(
            "source.ts-ms",
            DataTypes.STRING(),
            sourceFieldConverter(AbstractSourceInfo.TIMESTAMP_KEY, false)),

    /** Source offset timestamp in microseconds since the epoch. */
    SOURCE_TIMESTAMP_US(
            "source.ts-us",
            DataTypes.STRING(),
            offsetFieldConverter(SourceInfo.TIMESTAMP_USEC_KEY)),

    /** Canonical JSON containing the PostgreSQL source partition's stable server identity. */
    SOURCE_PARTITION("source.partition", DataTypes.STRING(), canonicalMapConverter(true)),

    /** Canonical JSON containing the safe PostgreSQL source offset fields. */
    SOURCE_OFFSET("source.offset", DataTypes.STRING(), canonicalMapConverter(false));

    private static final int MAX_METADATA_LENGTH = 4096;
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String TRANSACTION_ID_KEY = "transaction_id";
    private static final String[] SAFE_OFFSET_KEYS = {
        SourceInfo.LAST_SNAPSHOT_RECORD_KEY,
        SourceInfo.LSN_KEY,
        PostgresOffsetContext.LAST_COMMIT_LSN_KEY,
        PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY,
        SourceInfo.SNAPSHOT_KEY,
        TRANSACTION_ID_KEY,
        SourceInfo.TIMESTAMP_USEC_KEY,
        SourceInfo.TXID_KEY
    };
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String key;

    private final DataType dataType;

    private final MetadataConverter converter;

    PostgreSQLReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    private static MetadataConverter operationConverter() {
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object read(SourceRecord record) {
                Struct envelope = (Struct) record.value();
                return boundedString("source.op", envelope.getString(Envelope.FieldName.OPERATION));
            }
        };
    }

    private static MetadataConverter sourceFieldConverter(
            String sourceFieldName, boolean unsignedLong) {
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object read(SourceRecord record) {
                Struct source = source(record);
                if (source == null || source.schema().field(sourceFieldName) == null) {
                    return null;
                }
                Object value = source.get(sourceFieldName);
                if (value == null) {
                    return null;
                }
                String text =
                        unsignedLong
                                ? Long.toUnsignedString(((Number) value).longValue())
                                : String.valueOf(value);
                return boundedString("source." + sourceFieldName, text);
            }
        };
    }

    private static MetadataConverter offsetFieldConverter(String offsetFieldName) {
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object read(SourceRecord record) {
                Map<String, ?> offset = record.sourceOffset();
                if (offset == null || !offset.containsKey(offsetFieldName)) {
                    return null;
                }
                Object value = offset.get(offsetFieldName);
                return value == null
                        ? null
                        : boundedString("source." + offsetFieldName, String.valueOf(value));
            }
        };
    }

    private static MetadataConverter canonicalMapConverter(boolean partition) {
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object read(SourceRecord record) {
                Map<String, ?> values =
                        partition ? record.sourcePartition() : record.sourceOffset();
                if (values == null) {
                    return null;
                }
                TreeMap<String, Object> canonical = new TreeMap<>();
                if (partition) {
                    if (!values.containsKey(SERVER_PARTITION_KEY)
                            || values.get(SERVER_PARTITION_KEY) == null) {
                        return null;
                    }
                    canonical.put(
                            SERVER_PARTITION_KEY,
                            canonicalScalar("source.partition", values.get(SERVER_PARTITION_KEY)));
                } else {
                    for (String key : SAFE_OFFSET_KEYS) {
                        if (values.containsKey(key)) {
                            Object value = values.get(key);
                            canonical.put(
                                    key,
                                    isLsnKey(key) && value != null
                                            ? unsignedLsn(value)
                                            : canonicalScalar("source.offset", value));
                        }
                    }
                    if (canonical.isEmpty()) {
                        return null;
                    }
                }
                try {
                    return boundedString(
                            partition ? "source.partition" : "source.offset",
                            OBJECT_MAPPER.writeValueAsString(canonical));
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException(
                            "Failed to serialize PostgreSQL source metadata.", e);
                }
            }
        };
    }

    private static Struct source(SourceRecord record) {
        Struct envelope = (Struct) record.value();
        return envelope == null ? null : envelope.getStruct(Envelope.FieldName.SOURCE);
    }

    private static Object canonicalScalar(String metadataKey, Object value) {
        if (value == null || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof String) {
            requireBounded(metadataKey, (String) value);
            return value;
        }
        throw new IllegalArgumentException(
                "Unsupported value type in PostgreSQL metadata '" + metadataKey + "'.");
    }

    private static BigInteger unsignedLsn(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException(
                    "PostgreSQL source offset LSN must be represented as a number.");
        }
        return new BigInteger(Long.toUnsignedString(((Number) value).longValue()));
    }

    private static boolean isLsnKey(String key) {
        return SourceInfo.LSN_KEY.equals(key)
                || PostgresOffsetContext.LAST_COMMIT_LSN_KEY.equals(key)
                || PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY.equals(key);
    }

    private static StringData boundedString(String metadataKey, String value) {
        if (value == null) {
            return null;
        }
        requireBounded(metadataKey, value);
        return StringData.fromString(value);
    }

    private static void requireBounded(String metadataKey, String value) {
        if (value.length() > MAX_METADATA_LENGTH) {
            throw new IllegalArgumentException(
                    "PostgreSQL metadata '"
                            + metadataKey
                            + "' exceeds "
                            + MAX_METADATA_LENGTH
                            + " characters.");
        }
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }
}
