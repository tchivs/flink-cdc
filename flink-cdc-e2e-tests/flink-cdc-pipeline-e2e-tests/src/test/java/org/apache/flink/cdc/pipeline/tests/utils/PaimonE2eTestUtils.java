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

package org.apache.flink.cdc.pipeline.tests.utils;

import org.apache.flink.cdc.common.test.utils.TestUtils;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for querying Paimon tables from pipeline E2E Flink containers. */
public final class PaimonE2eTestUtils {

    private PaimonE2eTestUtils() {}

    public static void copySqlClientDependencies(
            GenericContainer<?> jobManager, String sharedVolume, String flinkVersion) {
        String paimonSqlConnectorResourceName = getPaimonSQLConnectorResourceName(flinkVersion);
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource(paimonSqlConnectorResourceName)),
                sharedVolume + "/" + paimonSqlConnectorResourceName);
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-shade-hadoop.jar")),
                sharedVolume + "/flink-shade-hadoop.jar");
    }

    public static List<String> fetchTableRows(
            GenericContainer<?> jobManager,
            String sharedVolume,
            String flinkVersion,
            String warehouse,
            String database,
            String table)
            throws Exception {
        String template =
                readLines("docker/peek-paimon.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, database, table);
        Container.ExecResult result =
                executeSql(
                        jobManager,
                        sharedVolume,
                        flinkVersion,
                        "peek.sql",
                        sql,
                        "Failed to execute peek script.");

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(PaimonE2eTestUtils::extractRow)
                .map(row -> String.format("%s", String.join(", ", row)))
                .collect(Collectors.toList());
    }

    public static List<String> fetchTables(
            GenericContainer<?> jobManager,
            String sharedVolume,
            String flinkVersion,
            String warehouse,
            String database)
            throws Exception {
        String sql =
                String.format(
                        "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                                + "CREATE CATALOG paimon_catalog WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ");\n"
                                + "SHOW TABLES IN paimon_catalog.%s;\n",
                        warehouse, database);
        Container.ExecResult result =
                executeSql(
                        jobManager,
                        sharedVolume,
                        flinkVersion,
                        "show-paimon-tables.sql",
                        sql,
                        "Failed to show Paimon tables.");

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(PaimonE2eTestUtils::extractRow)
                .filter(row -> row.length > 0)
                .map(row -> row[0])
                .collect(Collectors.toList());
    }

    public static String getPaimonSQLConnectorResourceName(String flinkVersion) {
        return String.format("paimon-sql-connector-%s.jar", flinkVersion);
    }

    private static Container.ExecResult executeSql(
            GenericContainer<?> jobManager,
            String sharedVolume,
            String flinkVersion,
            String fileName,
            String sql,
            String failureMessage)
            throws Exception {
        String containerSqlPath = sharedVolume + "/" + fileName;
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume + "/" + getPaimonSQLConnectorResourceName(flinkVersion),
                        "--jar",
                        sharedVolume + "/flink-shade-hadoop.jar",
                        "-f",
                        containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    failureMessage
                            + " Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }
        return result;
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = PaimonE2eTestUtils.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IOException("Resource not found: " + resource);
        }
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }
}
