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

import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE;

/** Utilities for resolving partition-aware PostgreSQL publication membership. */
public final class PartitionPublicationRefresher {

    private static final String AUTOCREATE_ALL_TABLES = "all_tables";
    private static final String AUTOCREATE_FILTERED = "filtered";
    private static final String AUTOCREATE_DISABLED = "disabled";

    public static final String ERR_PR_002 = "ERR-PR-002";
    public static final String ERR_PR_004 = "ERR-PR-004";
    public static final String ERR_PR_005 = "ERR-PR-005";
    public static final String ERR_PR_006 = "ERR-PR-006";
    public static final String ERR_PR_007 = "ERR-PR-007";

    private PartitionPublicationRefresher() {}

    public static List<TableId> computeDesiredMembers(
            Collection<TableId> requestedMembers, PartitionRoutingState routingState) {
        if (requestedMembers == null || requestedMembers.isEmpty()) {
            return new ArrayList<>();
        }
        PartitionRoutingState effectiveRoutingState =
                routingState == null ? PartitionRoutingState.EMPTY : routingState;
        Set<TableId> mixedMembers =
                effectiveRoutingState.findMixedParentChildCaptures(requestedMembers);
        if (!mixedMembers.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "[%s] Ambiguous publication SET TABLE list: resolved publication "
                                    + "tables = [%s]. Reason: PG13+ expands parent tables into "
                                    + "children inside pg_publication_tables; never include both "
                                    + "a parent and any of its children.",
                            ERR_PR_004, formatTableIds(mixedMembers)));
        }
        return new ArrayList<>(
                new LinkedHashSet<>(
                        PartitionCatalog.expandParentsToChildren(
                                requestedMembers, effectiveRoutingState)));
    }

    public static void validateDecoderbufsPublishViaPartitionRoot(
            String decoderName, boolean publishViaPartitionRoot, String publicationName) {
        if (!"decoderbufs".equalsIgnoreCase(decoderName) || !publishViaPartitionRoot) {
            return;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] Ineffective option combination: decoding.plugin.name=%s, "
                                + "publication=%s, publish_via_partition_root=true. Reason: "
                                + "publish_via_partition_root only affects the pgoutput "
                                + "logical-decoding translation layer; decoderbufs reads physical "
                                + "WAL and keeps child table identity. Remediation: use pgoutput "
                                + "for parent-root publication semantics, or drop "
                                + "publish_via_partition_root=true and rely on connector-side "
                                + "child-to-parent partition routing.",
                        ERR_PR_005, decoderName, publicationName));
    }

    public static void validatePartitionRoutingSupport(
            PostgresSourceConfig sourceConfig,
            PartitionRoutingState routingState,
            boolean publishViaPartitionRoot) {
        if (sourceConfig == null || routingState == null || routingState.isEmpty()) {
            return;
        }

        String decoderName = sourceConfig.getDbzProperties().getProperty("plugin.name");
        LogicalDecoderStrategy.fromPluginName(decoderName)
                .validateStartupMode(sourceConfig.getStartupOptions());

        String publicationName = sourceConfig.getDbzConnectorConfig().publicationName();
        validateDecoderbufsPublishViaPartitionRoot(
                decoderName, publishViaPartitionRoot, publicationName);

        String publicationAutocreateMode = publicationAutocreateMode(sourceConfig);
        boolean refreshEnabled = sourceConfig.partitionPublicationRefreshEnabled();
        if (refreshEnabled
                && (!"pgoutput".equalsIgnoreCase(decoderName)
                        || sourceConfig.getStartupOptions().startupMode == StartupMode.SNAPSHOT
                        || AUTOCREATE_ALL_TABLES.equals(publicationAutocreateMode))) {
            throw new IllegalArgumentException(
                    String.format(
                            "[%s] Unsupported publication refresh for partition routing: "
                                    + "scan.partition.publication.refresh.enabled=true, "
                                    + "decoding.plugin.name=%s, scan.startup.mode=%s, "
                                    + "debezium.publication.autocreate.mode=%s, publication=%s, "
                                    + "partition parents=[%s]. Reason: connector-side refresh mutates "
                                    + "PostgreSQL publication metadata and is only meaningful for "
                                    + "pgoutput streaming modes with filtered or disabled publication "
                                    + "membership. Support level: unsupported/fail-fast. "
                                    + "Remediation: disable scan.partition.publication.refresh.enabled, "
                                    + "or use pgoutput with initial/latest-offset/committed-offset and "
                                    + "debezium.publication.autocreate.mode=filtered or disabled.",
                            ERR_PR_007,
                            decoderName,
                            sourceConfig.getStartupOptions().startupMode,
                            publicationAutocreateMode,
                            publicationName,
                            formatTableIds(routingState.allParents())));
        }
        if (!"pgoutput".equalsIgnoreCase(decoderName)
                && !AUTOCREATE_ALL_TABLES.equals(publicationAutocreateMode)) {
            throw new IllegalArgumentException(
                    String.format(
                            "[%s] Unsupported publication mode for partition routing: "
                                    + "decoding.plugin.name=%s, scan.startup.mode=%s, "
                                    + "debezium.publication.autocreate.mode=%s, "
                                    + "publication=%s, partition parents=[%s]. "
                                    + "Reason: only pgoutput supports publication-specific "
                                    + "partition validation and refresh. Support level: "
                                    + "limited/static for decoderbufs. Remediation: use "
                                    + "decoding.plugin.name=pgoutput for publication-managed "
                                    + "partition routing, or keep decoderbufs with its default "
                                    + "publication mode and restart the job after adding new "
                                    + "child partitions.",
                            ERR_PR_006,
                            decoderName,
                            sourceConfig.getStartupOptions().startupMode,
                            publicationAutocreateMode,
                            publicationName,
                            formatTableIds(routingState.allParents())));
        }
        if (AUTOCREATE_FILTERED.equals(publicationAutocreateMode)) {
            computeDesiredMembers(routingState.allParents(), routingState);
        }
    }

    public static void validateExistingPublicationMembership(
            JdbcConnection jdbc,
            PostgresSourceConfig sourceConfig,
            PartitionRoutingState routingState,
            boolean publishViaPartitionRoot)
            throws SQLException {
        if (sourceConfig == null
                || routingState == null
                || routingState.isEmpty()
                || sourceConfig.partitionPublicationRefreshEnabled()) {
            return;
        }

        String publicationAutocreateMode = publicationAutocreateMode(sourceConfig);
        if (!AUTOCREATE_DISABLED.equals(publicationAutocreateMode) || publishViaPartitionRoot) {
            return;
        }

        List<TableId> missingChildren =
                missingPublicationMembers(
                        jdbc,
                        sourceConfig.getDbzConnectorConfig().publicationName(),
                        routingState.allChildren());
        if (missingChildren.isEmpty()) {
            return;
        }

        throw new IllegalArgumentException(
                String.format(
                        "[%s] Missing publication membership: publication=%s, "
                                + "missing children=[%s], decoding.plugin.name=%s, "
                                + "scan.startup.mode=%s, debezium.publication.autocreate.mode=%s. "
                                + "Support level: limited. Remediation: run as publication owner: "
                                + "ALTER PUBLICATION %s ADD TABLE %s; or set "
                                + "scan.partition.publication.refresh.enabled=true (pgoutput only).",
                        ERR_PR_002,
                        sourceConfig.getDbzConnectorConfig().publicationName(),
                        formatTableIds(missingChildren),
                        sourceConfig.getDbzProperties().getProperty("plugin.name"),
                        sourceConfig.getStartupOptions().startupMode,
                        publicationAutocreateMode,
                        sourceConfig.getDbzConnectorConfig().publicationName(),
                        missingChildren.stream()
                                .map(TableId::toDoubleQuotedString)
                                .collect(Collectors.joining(", "))));
    }

    public static void refreshInitialPublicationMembership(
            JdbcConnection jdbc,
            PostgresSourceConfig sourceConfig,
            PartitionRoutingState routingState,
            boolean publishViaPartitionRoot)
            throws SQLException {
        if (sourceConfig == null
                || routingState == null
                || routingState.isEmpty()
                || !sourceConfig.partitionPublicationRefreshEnabled()
                || publishViaPartitionRoot) {
            return;
        }
        if (!AUTOCREATE_DISABLED.equals(publicationAutocreateMode(sourceConfig))) {
            return;
        }
        refreshPublication(
                jdbc,
                sourceConfig.getDbzConnectorConfig().publicationName(),
                routingState.allChildren(),
                true);
    }

    public static void refreshPublication(
            JdbcConnection jdbc,
            String publicationName,
            Collection<TableId> childTables,
            boolean refreshEnabled)
            throws SQLException {
        if (!refreshEnabled || childTables == null || childTables.isEmpty()) {
            return;
        }
        Connection connection = jdbc.connection();
        boolean previousAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            List<TableId> missingChildren =
                    missingPublicationMembers(jdbc, publicationName, childTables);
            if (!missingChildren.isEmpty()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(buildAddTablesStatement(publicationName, missingChildren));
                }
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException(
                    String.format(
                            "[%s] Unable to refresh PostgreSQL publication %s for partition "
                                    + "children [%s]. Reason: ALTER PUBLICATION ADD TABLE requires "
                                    + "the connector user to own the publication or have sufficient "
                                    + "privileges. Remediation: run the job as publication owner, "
                                    + "manually add the missing child tables, or disable "
                                    + "scan.partition.publication.refresh.enabled.",
                            ERR_PR_007, publicationName, formatTableIds(childTables)),
                    e);
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    public static List<TableId> missingPublicationMembers(
            JdbcConnection jdbc, String publicationName, Collection<TableId> childTables)
            throws SQLException {
        List<TableId> missingChildren = new ArrayList<>();
        for (TableId childTable : childTables) {
            if (!isPublicationMember(jdbc, publicationName, childTable)) {
                missingChildren.add(childTable);
            }
        }
        return missingChildren;
    }

    static String buildAddTablesStatement(String publicationName, Collection<TableId> childTables) {
        return String.format(
                "ALTER PUBLICATION %s ADD TABLE %s;",
                quoteIdentifier(publicationName),
                childTables.stream()
                        .map(TableId::toDoubleQuotedString)
                        .collect(Collectors.joining(", ")));
    }

    private static boolean isPublicationMember(
            JdbcConnection jdbc, String publicationName, TableId tableId) throws SQLException {
        String query =
                "SELECT COUNT(1) FROM pg_publication_tables WHERE pubname = '"
                        + publicationName.replace("'", "''")
                        + "' AND schemaname = '"
                        + tableId.schema().replace("'", "''")
                        + "' AND tablename = '"
                        + tableId.table().replace("'", "''")
                        + "'";
        try (Statement statement = jdbc.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            return rs.next() && rs.getLong(1) > 0;
        }
    }

    public static boolean isPublishViaPartitionRootEnabled(
            io.debezium.jdbc.JdbcConnection jdbc, String publicationName) throws SQLException {
        if (!hasPublishViaPartitionRootColumn(jdbc)) {
            return false;
        }
        String query =
                "SELECT pubviaroot FROM pg_publication WHERE pubname = '"
                        + publicationName.replace("'", "''")
                        + "'";
        try (Statement statement = jdbc.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            return rs.next() && rs.getBoolean("pubviaroot");
        }
    }

    private static boolean hasPublishViaPartitionRootColumn(io.debezium.jdbc.JdbcConnection jdbc)
            throws SQLException {
        String query =
                "SELECT EXISTS ("
                        + "SELECT 1 FROM pg_attribute "
                        + "WHERE attrelid = 'pg_publication'::regclass "
                        + "AND attname = 'pubviaroot' "
                        + "AND NOT attisdropped)";
        try (Statement statement = jdbc.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    private static String publicationAutocreateMode(PostgresSourceConfig sourceConfig) {
        String configured =
                sourceConfig.getDbzProperties().getProperty(PUBLICATION_AUTOCREATE_MODE.name());
        if (configured == null || configured.trim().isEmpty()) {
            return PostgresConnectorConfig.AutoCreateMode.ALL_TABLES.getValue();
        }
        return configured.trim().toLowerCase();
    }

    private static String formatTableIds(Collection<TableId> tableIds) {
        return tableIds.stream().map(TableId::toString).sorted().collect(Collectors.joining(", "));
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
