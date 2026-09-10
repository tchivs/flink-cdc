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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.schema.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Immutable, validated configuration for Doris-visible CDC fields. */
final class DorisSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern CANONICAL_BIGINT = Pattern.compile("(?:0|-?[1-9][0-9]*)");
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private static final String SEQUENCE_COLUMN_OPTION =
            DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX + "function_column.sequence_col";

    private final DeleteMode deleteMode;
    private final String visibleDeleteColumn;
    private final List<MetadataColumn> metadataColumns;
    private final Map<String, String> reservedColumns;

    private DorisSinkConfig(
            DeleteMode deleteMode,
            String visibleDeleteColumn,
            List<MetadataColumn> metadataColumns) {
        this.deleteMode = deleteMode;
        this.visibleDeleteColumn = visibleDeleteColumn;
        this.metadataColumns = Collections.unmodifiableList(new ArrayList<>(metadataColumns));

        Map<String, String> columns = new HashMap<>();
        for (MetadataColumn metadataColumn : metadataColumns) {
            addReservedColumn(columns, metadataColumn.targetColumn);
        }
        if (deleteMode == DeleteMode.VISIBLE) {
            addReservedColumn(columns, visibleDeleteColumn);
        }
        this.reservedColumns = Collections.unmodifiableMap(columns);
    }

    static DorisSinkConfig from(Configuration configuration) {
        final DeleteMode deleteMode;
        String configuredDeleteMode = configuration.get(DorisDataSinkOptions.SINK_DELETE_MODE);
        try {
            deleteMode = DeleteMode.valueOf(configuredDeleteMode);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' must be PHYSICAL or VISIBLE, but was '%s'.",
                            DorisDataSinkOptions.SINK_DELETE_MODE.key(), configuredDeleteMode),
                    e);
        }

        String visibleDeleteColumn =
                configuration
                        .getOptional(DorisDataSinkOptions.SINK_VISIBLE_DELETE_COLUMN)
                        .orElse(null);
        if (visibleDeleteColumn != null) {
            validateIdentifier(
                    visibleDeleteColumn, DorisDataSinkOptions.SINK_VISIBLE_DELETE_COLUMN.key());
        }
        if (deleteMode == DeleteMode.VISIBLE && visibleDeleteColumn == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' is required when '%s' is VISIBLE.",
                            DorisDataSinkOptions.SINK_VISIBLE_DELETE_COLUMN.key(),
                            DorisDataSinkOptions.SINK_DELETE_MODE.key()));
        }

        Map<String, String> allOptions = configuration.toMap();
        String sequenceColumn = allOptions.get(SEQUENCE_COLUMN_OPTION);
        Map<String, Map<String, String>> columnOptions = new TreeMap<>();
        for (Map.Entry<String, String> option : allOptions.entrySet()) {
            if (!option.getKey().startsWith(DorisDataSinkOptions.SINK_METADATA_COLUMNS_PREFIX)) {
                continue;
            }
            String suffix =
                    option.getKey()
                            .substring(DorisDataSinkOptions.SINK_METADATA_COLUMNS_PREFIX.length());
            int propertySeparator = suffix.lastIndexOf('.');
            if (propertySeparator <= 0 || propertySeparator == suffix.length() - 1) {
                throw invalidMetadataOption(option.getKey());
            }
            String targetColumn = suffix.substring(0, propertySeparator);
            String property = suffix.substring(propertySeparator + 1);
            validateIdentifier(targetColumn, option.getKey());
            if (!property.equals("source")
                    && !property.equals("type")
                    && !property.equals("default")) {
                throw invalidMetadataOption(option.getKey());
            }
            columnOptions
                    .computeIfAbsent(targetColumn, ignored -> new HashMap<>())
                    .put(property, option.getValue());
        }

        List<MetadataColumn> metadataColumns = new ArrayList<>();
        Set<String> normalizedTargets = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : columnOptions.entrySet()) {
            String targetColumn = entry.getKey();
            String normalizedTarget = normalize(targetColumn);
            if (!normalizedTargets.add(normalizedTarget)) {
                throw new IllegalArgumentException(
                        "Metadata target column names must be unique ignoring case: "
                                + targetColumn);
            }
            Map<String, String> properties = entry.getValue();
            if (!properties.containsKey("source") || !properties.containsKey("type")) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata column '%s' must configure both '.source' and '.type'.",
                                targetColumn));
            }
            String sourceKey = properties.get("source");
            if (sourceKey == null || sourceKey.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata column '%s' must use a non-blank event metadata source key.",
                                targetColumn));
            }
            MetadataType type = parseMetadataType(targetColumn, properties.get("type"));
            boolean sequence =
                    sequenceColumn != null && sequenceColumn.equalsIgnoreCase(targetColumn);
            if (sequence && type != MetadataType.BIGINT) {
                throw new IllegalArgumentException(
                        String.format(
                                "Doris sequence column '%s' must be configured as BIGINT metadata.",
                                targetColumn));
            }
            boolean hasDefault = properties.containsKey("default");
            if (sequence && hasDefault) {
                throw new IllegalArgumentException(
                        String.format(
                                "Doris sequence metadata column '%s' must not configure a default; missing sequence metadata must fail.",
                                targetColumn));
            }
            String defaultLiteral = properties.get("default");
            Object defaultValue =
                    hasDefault ? type.convert(defaultLiteral, targetColumn, sequence) : null;
            metadataColumns.add(
                    new MetadataColumn(
                            targetColumn, sourceKey, type, hasDefault, defaultValue, sequence));
        }

        if (deleteMode == DeleteMode.VISIBLE) {
            String normalizedVisibleColumn = normalize(visibleDeleteColumn);
            for (MetadataColumn metadataColumn : metadataColumns) {
                if (normalize(metadataColumn.targetColumn).equals(normalizedVisibleColumn)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Visible delete column '%s' collides with a metadata target column.",
                                    visibleDeleteColumn));
                }
            }
            if (sequenceColumn != null && sequenceColumn.equalsIgnoreCase(visibleDeleteColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Visible delete column '%s' cannot be the Doris sequence column.",
                                visibleDeleteColumn));
            }
        }

        return new DorisSinkConfig(deleteMode, visibleDeleteColumn, metadataColumns);
    }

    boolean usesVisibleDeletes() {
        return deleteMode == DeleteMode.VISIBLE;
    }

    String getVisibleDeleteColumn() {
        return visibleDeleteColumn;
    }

    List<MetadataColumn> getMetadataColumns() {
        return metadataColumns;
    }

    void validateSchema(Schema schema) {
        validateColumnNames(schema.getColumnNames());
    }

    void validateColumnNames(Iterable<String> columnNames) {
        for (String columnName : columnNames) {
            if (DORIS_DELETE_SIGN.equalsIgnoreCase(columnName)) {
                throw new IllegalArgumentException(
                        String.format("Source column '%s' is reserved by Doris.", columnName));
            }
            String configuredColumn = reservedColumns.get(normalize(columnName));
            if (configuredColumn != null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Source column '%s' collides with configured Doris column '%s'.",
                                columnName, configuredColumn));
            }
        }
    }

    private static void addReservedColumn(Map<String, String> columns, String columnName) {
        if (DORIS_DELETE_SIGN.equalsIgnoreCase(columnName)) {
            throw new IllegalArgumentException(
                    String.format("Configured column '%s' is reserved by Doris.", columnName));
        }
        String previous = columns.put(normalize(columnName), columnName);
        if (previous != null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Configured Doris columns '%s' and '%s' collide ignoring case.",
                            previous, columnName));
        }
    }

    private static MetadataType parseMetadataType(String targetColumn, String configuredType) {
        try {
            return MetadataType.valueOf(configuredType);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Metadata column '%s' type must be BIGINT, BOOLEAN, or STRING, but was '%s'.",
                            targetColumn, configuredType),
                    e);
        }
    }

    private static void validateIdentifier(String identifier, String optionKey) {
        if (!SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' contains unsafe Doris identifier '%s'.",
                            optionKey, identifier));
        }
    }

    private static IllegalArgumentException invalidMetadataOption(String optionKey) {
        return new IllegalArgumentException(
                String.format(
                        "Option '%s' must match '%s<target>.source|type|default'.",
                        optionKey, DorisDataSinkOptions.SINK_METADATA_COLUMNS_PREFIX));
    }

    private static String normalize(String columnName) {
        return columnName.toLowerCase(Locale.ROOT);
    }

    private enum DeleteMode {
        PHYSICAL,
        VISIBLE
    }

    enum MetadataType {
        BIGINT {
            @Override
            Object convert(String value, String targetColumn, boolean sequence) {
                if (value == null || value.trim().isEmpty()) {
                    throw invalidValue(targetColumn, value, "a non-blank BIGINT");
                }
                if (!CANONICAL_BIGINT.matcher(value).matches()) {
                    throw invalidValue(targetColumn, value, "a canonical BIGINT");
                }
                final long parsed;
                try {
                    parsed = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw invalidValue(targetColumn, value, "a BIGINT", e);
                }
                if (sequence && parsed < 0) {
                    throw invalidValue(targetColumn, value, "a non-negative BIGINT sequence value");
                }
                return parsed;
            }
        },
        BOOLEAN {
            @Override
            Object convert(String value, String targetColumn, boolean sequence) {
                if ("true".equals(value)) {
                    return true;
                }
                if ("false".equals(value)) {
                    return false;
                }
                throw invalidValue(targetColumn, value, "canonical true or false");
            }
        },
        STRING {
            @Override
            Object convert(String value, String targetColumn, boolean sequence) {
                if (value == null) {
                    throw invalidValue(targetColumn, null, "a STRING");
                }
                return value;
            }
        };

        abstract Object convert(String value, String targetColumn, boolean sequence);

        private static IllegalArgumentException invalidValue(
                String targetColumn, String value, String expectation) {
            return invalidValue(targetColumn, value, expectation, null);
        }

        private static IllegalArgumentException invalidValue(
                String targetColumn, String value, String expectation, Exception cause) {
            return new IllegalArgumentException(
                    String.format(
                            "Metadata value '%s' for target column '%s' must be %s.",
                            value, targetColumn, expectation),
                    cause);
        }
    }

    static final class MetadataColumn implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String targetColumn;
        private final String sourceKey;
        private final MetadataType type;
        private final boolean hasDefault;
        private final Object defaultValue;
        private final boolean sequence;

        private MetadataColumn(
                String targetColumn,
                String sourceKey,
                MetadataType type,
                boolean hasDefault,
                Object defaultValue,
                boolean sequence) {
            this.targetColumn = targetColumn;
            this.sourceKey = sourceKey;
            this.type = type;
            this.hasDefault = hasDefault;
            this.defaultValue = defaultValue;
            this.sequence = sequence;
        }

        String getTargetColumn() {
            return targetColumn;
        }

        String getDdlType() {
            String ddlType = type.name() + " NOT NULL";
            if (hasDefault && type == MetadataType.STRING) {
                return ddlType + " DEFAULT " + quoteStringLiteral((String) defaultValue);
            }
            return ddlType;
        }

        String getDdlDefaultValue() {
            return hasDefault && type != MetadataType.STRING ? String.valueOf(defaultValue) : null;
        }

        private static String quoteStringLiteral(String value) {
            return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
        }

        Object read(Map<String, String> eventMetadata) {
            String value = eventMetadata.get(sourceKey);
            if (value == null) {
                if (hasDefault) {
                    return defaultValue;
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Event metadata key '%s' required by Doris target column '%s' is missing.",
                                sourceKey, targetColumn));
            }
            return type.convert(value, targetColumn, sequence);
        }
    }
}
