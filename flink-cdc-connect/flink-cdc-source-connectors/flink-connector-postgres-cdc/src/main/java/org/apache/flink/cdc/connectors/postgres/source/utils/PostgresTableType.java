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

/**
 * Enumeration representing PostgreSQL table types supported by CDC operations.
 *
 * <p>This enum provides type-safe handling of PostgreSQL table types and includes utility methods
 * for checking CDC support and partition status.
 */
public enum PostgresTableType {
    /** Regular table type. */
    TABLE("TABLE"),

    /** Partitioned table type (PostgreSQL 10+). */
    PARTITIONED_TABLE("PARTITIONED TABLE"),

    /** View type. */
    VIEW("VIEW"),

    /** Materialized view type. */
    MATERIALIZED_VIEW("MATERIALIZED VIEW"),

    /** Unknown or unsupported table type. */
    UNKNOWN("UNKNOWN");

    private final String jdbcTypeName;

    PostgresTableType(String jdbcTypeName) {
        this.jdbcTypeName = jdbcTypeName;
    }

    /**
     * Gets the JDBC type name as returned by DatabaseMetaData.getTables().
     *
     * @return the JDBC type name
     */
    public String getJdbcTypeName() {
        return jdbcTypeName;
    }

    /**
     * Checks if this table type represents a partitioned table.
     *
     * @return true if this is a partitioned table
     */
    public boolean isPartitioned() {
        return this == PARTITIONED_TABLE;
    }

    /**
     * Checks if this table type is supported for CDC operations.
     *
     * @return true if CDC operations are supported for this table type
     */
    public boolean isCdcSupported() {
        return this == TABLE || this == PARTITIONED_TABLE;
    }

    /**
     * Converts a JDBC type name to the corresponding PostgresTableType enum.
     *
     * @param jdbcTypeName the JDBC type name from DatabaseMetaData
     * @return the corresponding enum value, or UNKNOWN if not found
     */
    public static PostgresTableType fromJdbcTypeName(String jdbcTypeName) {
        if (jdbcTypeName == null) {
            return UNKNOWN;
        }

        for (PostgresTableType type : values()) {
            if (type.jdbcTypeName.equals(jdbcTypeName)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    /**
     * Gets an array of JDBC type names for CDC-supported table types.
     *
     * @return array of JDBC type names for CDC operations
     */
    public static String[] getCdcSupportedTypes() {
        return new String[] {TABLE.jdbcTypeName, PARTITIONED_TABLE.jdbcTypeName};
    }

    /**
     * Gets an array of all supported JDBC type names.
     *
     * @return array of all JDBC type names except UNKNOWN
     */
    public static String[] getAllSupportedTypes() {
        return new String[] {
            TABLE.jdbcTypeName,
            PARTITIONED_TABLE.jdbcTypeName,
            VIEW.jdbcTypeName,
            MATERIALIZED_VIEW.jdbcTypeName
        };
    }
}
