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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.util.Locale;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Plugin-specific behavior for PostgreSQL logical decoders in partition-routing mode. */
public enum LogicalDecoderStrategy {
    PGOUTPUT("pgoutput") {
        @Override
        public boolean supportsPublicationRefresh() {
            return true;
        }

        @Override
        public boolean requiresPublicationMembershipValidation() {
            return true;
        }

        @Override
        public boolean supportsRelationMessageDiscovery() {
            return true;
        }

        @Override
        public boolean supportsRuntimeChildDiscovery() {
            return true;
        }
    },

    DECODERBUFS("decoderbufs") {
        @Override
        public RuntimeDiscoveryAction resolveRuntimeDiscoveryAction(
                StartupOptions startupOptions, boolean childDiscoveredDuringStartup) {
            return childDiscoveredDuringStartup
                    ? RuntimeDiscoveryAction.ROUTE_WITH_STATIC_SEED
                    : RuntimeDiscoveryAction.FAIL_FAST_UNKNOWN_CHILD;
        }
    };

    private final String pluginName;

    LogicalDecoderStrategy(String pluginName) {
        this.pluginName = pluginName;
    }

    public static LogicalDecoderStrategy fromPluginName(String pluginName) {
        checkNotNull(pluginName, "pluginName must not be null");
        String normalizedPluginName = pluginName.trim().toLowerCase(Locale.ROOT);
        for (LogicalDecoderStrategy strategy : values()) {
            if (strategy.pluginName.equals(normalizedPluginName)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported PostgreSQL logical decoder for partition routing: %s. "
                                + "Supported values are pgoutput and decoderbufs.",
                        pluginName));
    }

    public String pluginName() {
        return pluginName;
    }

    public boolean supportsPublicationRefresh() {
        return false;
    }

    public boolean requiresPublicationMembershipValidation() {
        return false;
    }

    public boolean supportsRelationMessageDiscovery() {
        return false;
    }

    public boolean supportsRuntimeChildDiscovery() {
        return false;
    }

    public void validatePublicationMode(String publicationAutocreateMode) {
        if (supportsPublicationRefresh()) {
            return;
        }
        if (publicationAutocreateMode != null && !publicationAutocreateMode.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "PostgreSQL logical decoder %s does not use PostgreSQL publications; "
                                    + "publication mode %s is not applicable to partition routing.",
                            pluginName, publicationAutocreateMode));
        }
    }

    public void validateStartupMode(StartupOptions startupOptions) {
        checkNotNull(startupOptions, "startupOptions must not be null");
    }

    public RuntimeDiscoveryAction resolveRuntimeDiscoveryAction(
            StartupOptions startupOptions, boolean childDiscoveredDuringStartup) {
        validateStartupMode(startupOptions);
        return RuntimeDiscoveryAction.DISCOVER_AND_ROUTE;
    }

    /** Action to take when a streaming record references a partition child. */
    public enum RuntimeDiscoveryAction {
        DISCOVER_AND_ROUTE,
        ROUTE_WITH_STATIC_SEED,
        FAIL_FAST_UNKNOWN_CHILD
    }
}
