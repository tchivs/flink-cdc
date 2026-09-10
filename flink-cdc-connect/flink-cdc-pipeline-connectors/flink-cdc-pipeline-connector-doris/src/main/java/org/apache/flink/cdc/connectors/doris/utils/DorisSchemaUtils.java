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

package org.apache.flink.cdc.connectors.doris.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SCHEMA_CHANGE_ALLOWED_TYPES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_DEFAULT_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_DEFAULT_PARTITION_UNIT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_EXCLUDE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_INCLUDE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_UNIT;

/** Utilities for doris schema. */
public class DorisSchemaUtils {

    public static final String DEFAULT_DATE = "1970-01-01";
    public static final String DEFAULT_DATETIME = "1970-01-01 00:00:00";

    public static final String INVALID_OR_MISSING_DATATIME = "0000-00-00 00:00:00";

    private static final Set<SchemaChangeEventType> SUPPORTED_SCHEMA_EVOLUTION_TYPES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            CREATE_TABLE,
                            ADD_COLUMN,
                            ALTER_COLUMN_TYPE,
                            DROP_COLUMN,
                            DROP_TABLE,
                            RENAME_COLUMN,
                            TRUNCATE_TABLE));

    private static final String SUPPORTED_SCHEMA_EVOLUTION_TYPE_NAMES =
            SUPPORTED_SCHEMA_EVOLUTION_TYPES.stream()
                    .map(Enum::name)
                    .collect(Collectors.joining(", "));

    public static Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return SUPPORTED_SCHEMA_EVOLUTION_TYPES;
    }

    /** Returns the configured allowlist, always retaining CREATE_TABLE for initial provisioning. */
    public static Set<SchemaChangeEventType> getAllowedSchemaEvolutionTypes(Configuration config) {
        Optional<String> configuredTypes = config.getOptional(SCHEMA_CHANGE_ALLOWED_TYPES);
        if (!configuredTypes.isPresent()) {
            return SUPPORTED_SCHEMA_EVOLUTION_TYPES;
        }

        EnumSet<SchemaChangeEventType> allowedTypes = EnumSet.noneOf(SchemaChangeEventType.class);
        String[] typeNames = configuredTypes.get().split(",", -1);
        for (String rawTypeName : typeNames) {
            String typeName = rawTypeName.trim();
            if (typeName.isEmpty()) {
                throw invalidSchemaEvolutionTypes();
            }

            final SchemaChangeEventType eventType;
            try {
                eventType = SchemaChangeEventType.valueOf(typeName);
            } catch (IllegalArgumentException e) {
                throw invalidSchemaEvolutionTypes(e);
            }
            if (!SUPPORTED_SCHEMA_EVOLUTION_TYPES.contains(eventType)) {
                throw invalidSchemaEvolutionTypes();
            }
            allowedTypes.add(eventType);
        }

        allowedTypes.add(CREATE_TABLE);
        return Collections.unmodifiableSet(allowedTypes);
    }

    private static IllegalArgumentException invalidSchemaEvolutionTypes() {
        return invalidSchemaEvolutionTypes(null);
    }

    private static IllegalArgumentException invalidSchemaEvolutionTypes(Exception cause) {
        return new IllegalArgumentException(
                String.format(
                        "Option '%s' must be a non-empty, comma-separated list containing only: [%s].",
                        SCHEMA_CHANGE_ALLOWED_TYPES.key(), SUPPORTED_SCHEMA_EVOLUTION_TYPE_NAMES),
                cause);
    }

    /**
     * Get partition info by config. Currently only supports DATE/TIMESTAMP AUTO RANGE PARTITION and
     * doris version should greater than 2.1.6
     *
     * @param config
     * @param schema
     * @param tableId
     * @return
     */
    public static Tuple2<String, String> getPartitionInfo(
            Configuration config, Schema schema, TableId tableId) {
        Map<String, String> autoPartitionProperties =
                DorisDataSinkOptions.getPropertiesByPrefix(
                        config, TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX);
        if (autoPartitionProperties.isEmpty()) {
            return null;
        }

        if (isExcluded(autoPartitionProperties, tableId)
                || !isIncluded(autoPartitionProperties, tableId)) {
            return null;
        }

        String partitionKey =
                getPartitionProperty(
                        autoPartitionProperties,
                        tableId,
                        TABLE_CREATE_PARTITION_KEY,
                        TABLE_CREATE_DEFAULT_PARTITION_KEY);
        if (partitionKey == null || !schema.getColumn(partitionKey).isPresent()) {
            return null;
        }

        String partitionUnit =
                getPartitionProperty(
                        autoPartitionProperties,
                        tableId,
                        TABLE_CREATE_PARTITION_UNIT,
                        TABLE_CREATE_DEFAULT_PARTITION_UNIT);
        if (partitionUnit == null) {
            return null;
        }

        DataType dataType = schema.getColumn(partitionKey).get().getType();
        return isValidDataType(dataType) ? new Tuple2<>(partitionKey, partitionUnit) : null;
    }

    private static boolean isExcluded(Map<String, String> properties, TableId tableId) {
        String excludes = properties.get(TABLE_CREATE_PARTITION_EXCLUDE);
        if (!StringUtils.isNullOrWhitespaceOnly(excludes)) {
            Selectors selectExclude =
                    new Selectors.SelectorsBuilder().includeTables(excludes).build();
            return selectExclude.isMatch(tableId);
        }
        return false;
    }

    private static boolean isIncluded(Map<String, String> properties, TableId tableId) {
        String includes = properties.get(TABLE_CREATE_PARTITION_INCLUDE);
        if (!StringUtils.isNullOrWhitespaceOnly(includes)) {
            Selectors selectInclude =
                    new Selectors.SelectorsBuilder().includeTables(includes).build();
            return selectInclude.isMatch(tableId);
        }
        return true;
    }

    private static String getPartitionProperty(
            Map<String, String> properties,
            TableId tableId,
            String specificKey,
            String defaultKey) {
        String key = properties.get(tableId.identifier() + "." + specificKey);
        return StringUtils.isNullOrWhitespaceOnly(key) ? properties.get(defaultKey) : key;
    }

    private static boolean isValidDataType(DataType dataType) {
        return dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType
                || dataType instanceof DateType;
    }
}
