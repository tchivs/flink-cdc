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

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionGuard}. */
class PartitionGuardTest {

    private static final List<String> CONFIGURED =
            Collections.singletonList("public.aia_t_icc_jjdb");

    private static final PartitionGuard UNREADABLE_CATALOG =
            new PartitionGuard(
                    PostgresPartitionRouting.create(
                            Collections.singletonMap(
                                    "public.aia_t_icc_jjdb_202610",
                                    Collections.singleton("public.aia_t_icc_jjdb")),
                            "aiadb",
                            CONFIGURED),
                    "aiadb",
                    CONFIGURED,
                    () -> {
                        throw new SQLException("Connection refused");
                    });

    @Test
    void testUnreadableCatalogFailsInsteadOfLettingThePartitionThrough() {
        assertThatThrownBy(
                        () ->
                                UNREADABLE_CATALOG.checkFiltered(
                                        new TableId(null, "public", "aia_t_icc_jjdb_202611")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot verify")
                .hasCauseInstanceOf(SQLException.class);
        assertThatThrownBy(
                        () ->
                                UNREADABLE_CATALOG.checkRouted(
                                        new TableId(null, "public", "aia_t_icc_jjdb_202610")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot verify")
                .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void testCapturedTableThatIsNotRoutedNeedsNoCatalog() {
        assertThatCode(
                        () ->
                                UNREADABLE_CATALOG.checkRouted(
                                        new TableId(null, "public", "aia_t_icc_other")))
                .doesNotThrowAnyException();
    }
}
