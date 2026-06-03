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

package org.apache.flink.cdc.connectors.paimon.sink.utils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Paimon catalog operations with bounded retry logic.
 *
 * <p>The TaskManager-side Paimon catalog may briefly lag behind the JobManager-side metadata
 * applier, especially for filesystem catalogs without a shared warehouse volume. This utility
 * provides a standard retry mechanism to tolerate such transient visibility delays.
 */
public final class PaimonCatalogRetryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonCatalogRetryUtils.class);

    /** Maximum number of catalog lookup retries when a Paimon table is not yet visible. */
    public static final int DEFAULT_MAX_RETRIES = 5;

    /** Default interval between catalog lookup retries, in milliseconds. */
    public static final long DEFAULT_RETRY_INTERVAL_MS = 1_000L;

    private PaimonCatalogRetryUtils() {}

    /**
     * Look up a Paimon table from the catalog with bounded retries using default settings.
     *
     * @param catalog the Paimon catalog to query
     * @param identifier the table identifier to look up
     * @param callerContext a descriptive label of the caller (used in log messages)
     * @return the resolved Table
     * @throws RuntimeException if the table is not visible after all retries or an unexpected error
     *     occurs
     */
    public static Table getTableWithRetry(
            Catalog catalog, Identifier identifier, String callerContext) {
        return getTableWithRetry(
                catalog, identifier, callerContext, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL_MS);
    }

    /**
     * Look up a Paimon table from the catalog with bounded retries.
     *
     * @param catalog the Paimon catalog to query
     * @param identifier the table identifier to look up
     * @param callerContext a descriptive label of the caller (used in log messages)
     * @param maxRetries maximum number of retry attempts
     * @param retryIntervalMs interval between retries in milliseconds
     * @return the resolved Table
     * @throws RuntimeException if the table is not visible after all retries or an unexpected error
     *     occurs
     */
    public static Table getTableWithRetry(
            Catalog catalog,
            Identifier identifier,
            String callerContext,
            int maxRetries,
            long retryIntervalMs) {
        Catalog.TableNotExistException lastNotExist = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                lastNotExist = e;
                LOG.warn(
                        "[{}] Paimon table {} not yet visible (attempt {}/{}). Retry in {} ms.",
                        callerContext,
                        identifier,
                        attempt,
                        maxRetries,
                        retryIntervalMs);
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Interrupted while waiting for Paimon table "
                                    + identifier
                                    + " to become visible.",
                            ie);
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Unexpected error while loading Paimon table " + identifier, e);
            }
        }
        throw new RuntimeException(
                "Paimon table "
                        + identifier
                        + " did not become visible after "
                        + maxRetries
                        + " retries. Most likely the JobManager and TaskManager do not share "
                        + "the same filesystem catalog warehouse; please mount a shared volume.",
                lastNotExist);
    }
}
