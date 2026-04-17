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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Unit test for {@link Pg10CaptureState} and {@link Pg10StreamingSessionRuntime}. */
class PostgresStreamFetchTaskTest {

    @Test
    void testCaptureStateIsImmutable() {
        TableId parent = new TableId(null, "schema", "parent");
        TableId child1 = new TableId(null, "schema", "child1");
        Map<TableId, TableId> childToParent = new java.util.HashMap<>();
        childToParent.put(child1, parent);
        Map<TableId, java.util.List<TableId>> parentToChildren = new java.util.HashMap<>();
        parentToChildren.put(parent, java.util.Collections.singletonList(child1));

        Pg10CaptureState state = Pg10CaptureState.of(childToParent, parentToChildren);

        Assertions.assertThatThrownBy(
                        () ->
                                state.getChildToParentMapping()
                                        .put(new TableId(null, "s", "x"), parent))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSequentialCaptureStateBaselinesDoNotReacceptPreviouslyAcceptedChildren() {
        TableId parentProducts = new TableId(null, "inventory_partitioned", "products");
        TableId childUk = new TableId(null, "inventory_partitioned", "products_uk");
        TableId childCa = new TableId(null, "inventory_partitioned", "products_ca");
        TableId childAu = new TableId(null, "inventory_partitioned", "products_au");

        Pg10CaptureState original =
                Pg10CaptureState.of(
                        Collections.singletonMap(childUk, parentProducts),
                        Collections.singletonMap(
                                parentProducts, Collections.singletonList(childUk)));
        Pg10CaptureState afterCa =
                Pg10CaptureState.of(
                        new LinkedHashMap<TableId, TableId>() {
                            {
                                put(childUk, parentProducts);
                                put(childCa, parentProducts);
                            }
                        },
                        Collections.singletonMap(parentProducts, Arrays.asList(childUk, childCa)));

        Pg10CaptureState afterAu =
                Pg10CaptureState.of(
                        new LinkedHashMap<TableId, TableId>() {
                            {
                                put(childUk, parentProducts);
                                put(childCa, parentProducts);
                                put(childAu, parentProducts);
                            }
                        },
                        Collections.singletonMap(
                                parentProducts, Arrays.asList(childUk, childCa, childAu)));

        Map<TableId, TableId> newChildrenFromFirstRestart =
                new java.util.HashMap<>(afterCa.getChildToParentMapping());
        newChildrenFromFirstRestart.keySet().removeAll(original.getChildToParentMapping().keySet());

        Map<TableId, TableId> newChildrenFromStaleSecondRestart =
                new java.util.HashMap<>(afterAu.getChildToParentMapping());
        newChildrenFromStaleSecondRestart
                .keySet()
                .removeAll(original.getChildToParentMapping().keySet());

        Map<TableId, TableId> newChildrenFromSecondRestart =
                new java.util.HashMap<>(afterAu.getChildToParentMapping());
        newChildrenFromSecondRestart.keySet().removeAll(afterCa.getChildToParentMapping().keySet());

        Assertions.assertThat(newChildrenFromFirstRestart).containsOnlyKeys(childCa);
        Assertions.assertThat(newChildrenFromStaleSecondRestart).containsOnlyKeys(childCa, childAu);
        Assertions.assertThat(newChildrenFromSecondRestart).containsOnlyKeys(childAu);
    }

    @Test
    void testShouldTriggerRestartForPublishedButUnacceptedChild() {
        TableId parent = new TableId(null, "inventory", "products");
        TableId acceptedChild = new TableId(null, "inventory", "products_uk");
        TableId publishedButUnacceptedChild = new TableId(null, "inventory", "products_ca");
        Pg10CaptureState acceptedCaptureState =
                Pg10CaptureState.of(
                        Collections.singletonMap(acceptedChild, parent),
                        Collections.singletonMap(parent, Collections.singletonList(acceptedChild)));
        Set<TableId> publishedButUnacceptedChildren =
                Collections.singleton(publishedButUnacceptedChild);

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerRestartForRelation(
                                publishedButUnacceptedChild,
                                acceptedCaptureState.getChildToParentMapping(),
                                publishedButUnacceptedChildren))
                .isTrue();
    }

    @Test
    void testShouldNotTriggerRestartForSameSchemaTableOutsidePublishedChildren() {
        TableId parent = new TableId(null, "inventory", "products");
        TableId acceptedChild = new TableId(null, "inventory", "products_uk");
        TableId sameSchemaButNotPublishedChild = new TableId(null, "inventory", "audit_log");
        Pg10CaptureState acceptedCaptureState =
                Pg10CaptureState.of(
                        Collections.singletonMap(acceptedChild, parent),
                        Collections.singletonMap(parent, Collections.singletonList(acceptedChild)));
        Set<TableId> publishedButUnacceptedChildren =
                Collections.singleton(new TableId(null, "inventory", "products_ca"));

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerRestartForRelation(
                                sameSchemaButNotPublishedChild,
                                acceptedCaptureState.getChildToParentMapping(),
                                publishedButUnacceptedChildren))
                .isFalse();
    }

    @Test
    void testShouldNotTriggerRestartForAlreadyAcceptedChild() {
        TableId parent = new TableId(null, "inventory", "products");
        TableId acceptedChild = new TableId(null, "inventory", "products_uk");
        Pg10CaptureState acceptedCaptureState =
                Pg10CaptureState.of(
                        Collections.singletonMap(acceptedChild, parent),
                        Collections.singletonMap(parent, Collections.singletonList(acceptedChild)));
        Set<TableId> publishedButUnacceptedChildren = Set.of(acceptedChild);

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerRestartForRelation(
                                acceptedChild,
                                acceptedCaptureState.getChildToParentMapping(),
                                publishedButUnacceptedChildren))
                .isFalse();
    }

    @Test
    void testShouldNotTriggerRestartWhenPublishedChildSetEmpty() {
        TableId parent = new TableId(null, "inventory", "products");
        TableId acceptedChild = new TableId(null, "inventory", "products_uk");
        TableId unseenChild = new TableId(null, "inventory", "products_ca");
        Pg10CaptureState acceptedCaptureState =
                Pg10CaptureState.of(
                        Collections.singletonMap(acceptedChild, parent),
                        Collections.singletonMap(parent, Collections.singletonList(acceptedChild)));

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerRestartForRelation(
                                unseenChild,
                                acceptedCaptureState.getChildToParentMapping(),
                                Collections.<TableId>emptySet()))
                .isFalse();
    }

    @Test
    void testRestartOffsetIsFrozenAtLastCommittedBoundary() {
        PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        PostgresOffsetContext offsetContext =
                new PostgresOffsetContext.Loader(connectorConfig).load(offsetValues);

        PostgresOffsetContext frozenRestartOffset =
                Pg10StreamFetchTask.freezeRestartOffsetAtLastCommit(offsetContext, connectorConfig);

        Assertions.assertThat(
                        io.debezium.connector.postgresql.Utils.lastKnownLsn(frozenRestartOffset))
                .isEqualTo(Lsn.valueOf(100L));
        Assertions.assertThat(
                        frozenRestartOffset
                                .getOffset()
                                .get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY))
                .isEqualTo(100L);
    }

    @Test
    void testRestartOffsetSnapshotDoesNotReuseMutableOffsetContext() {
        PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 100L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 100L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        PostgresOffsetContext mutableOffsetContext =
                new PostgresOffsetContext.Loader(connectorConfig).load(offsetValues);

        PostgresOffsetContext frozenRestartOffset =
                Pg10StreamFetchTask.freezeRestartOffsetAtLastCommit(
                        mutableOffsetContext, connectorConfig);

        mutableOffsetContext.updateWalPosition(
                Lsn.valueOf(200L), Lsn.valueOf(200L), Instant.ofEpochMilli(1L), null, null, null);

        Assertions.assertThat(frozenRestartOffset).isNotSameAs(mutableOffsetContext);
        Assertions.assertThat(
                        io.debezium.connector.postgresql.Utils.lastKnownLsn(frozenRestartOffset))
                .isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    void testRelationTriggeredRestartClosesReplicationConnection() throws Exception {
        TableId childCa = new TableId(null, "inventory", "products_ca");
        AtomicBoolean refreshRequested = new AtomicBoolean(false);
        AtomicReference<TableId> restartTriggerTable = new AtomicReference<>();
        AtomicReference<String> restartTriggerSource = new AtomicReference<>();
        AtomicReference<PostgresOffsetContext> frozenRestartOffset = new AtomicReference<>();
        PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        PostgresOffsetContext currentOffsetContext =
                new PostgresOffsetContext.Loader(connectorConfig).load(offsetValues);
        ReplicationConnection replicationConnection = Mockito.mock(ReplicationConnection.class);

        AtomicBoolean stopCalled = new AtomicBoolean(false);
        PostgresStreamFetchTask.StoppableChangeEventSourceContext sessionContext =
                new PostgresStreamFetchTask.StoppableChangeEventSourceContext() {
                    @Override
                    public void stopChangeEventSource() {
                        stopCalled.set(true);
                        super.stopChangeEventSource();
                    }
                };

        Method requestRestart =
                Pg10StreamFetchTask.class.getDeclaredMethod(
                        "requestRestart",
                        TableId.class,
                        String.class,
                        PostgresOffsetContext.class,
                        PostgresConnectorConfig.class,
                        PostgresStreamFetchTask.StoppableChangeEventSourceContext.class,
                        AtomicBoolean.class,
                        AtomicReference.class,
                        AtomicReference.class,
                        AtomicReference.class,
                        ReplicationConnection.class);
        requestRestart.setAccessible(true);

        boolean triggered =
                (boolean)
                        requestRestart.invoke(
                                null,
                                childCa,
                                "relation",
                                currentOffsetContext,
                                connectorConfig,
                                sessionContext,
                                refreshRequested,
                                restartTriggerTable,
                                restartTriggerSource,
                                frozenRestartOffset,
                                replicationConnection);

        Assertions.assertThat(triggered).isTrue();
        Assertions.assertThat(refreshRequested).isTrue();
        Assertions.assertThat(restartTriggerTable).hasValue(childCa);
        Assertions.assertThat(restartTriggerSource).hasValue("relation");
        Assertions.assertThat(stopCalled).isTrue();
        Mockito.verify(replicationConnection).close();
        Assertions.assertThat(
                        io.debezium.connector.postgresql.Utils.lastKnownLsn(
                                frozenRestartOffset.get()))
                .isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    void testConcurrentRestartAttemptsOnlyAllowOneWinner() throws Exception {
        TableId relationChild = new TableId(null, "inventory", "products_ca");
        TableId pollerChild = new TableId(null, "inventory", "products_au");
        AtomicBoolean refreshRequested = new AtomicBoolean(false);
        AtomicReference<TableId> restartTriggerTable = new AtomicReference<>();
        AtomicReference<String> restartTriggerSource = new AtomicReference<>();
        AtomicReference<PostgresOffsetContext> frozenRestartOffset = new AtomicReference<>();
        PostgresConnectorConfig connectorConfig =
                new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 200L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        PostgresOffsetContext currentOffsetContext = Mockito.mock(PostgresOffsetContext.class);
        AtomicInteger freezeEnterCount = new AtomicInteger(0);
        AtomicBoolean releaseFreeze = new AtomicBoolean(false);
        ReplicationConnection replicationConnection = Mockito.mock(ReplicationConnection.class);

        Mockito.when(currentOffsetContext.getOffset())
                .thenAnswer(
                        invocation -> {
                            freezeEnterCount.incrementAndGet();
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                            while (!releaseFreeze.get() && System.nanoTime() < deadline) {
                                Thread.sleep(10L);
                            }
                            Assertions.assertThat(releaseFreeze)
                                    .as("freeze gate should be released before timeout")
                                    .isTrue();
                            return offsetValues;
                        });

        Method requestRestart =
                Pg10StreamFetchTask.class.getDeclaredMethod(
                        "requestRestart",
                        TableId.class,
                        String.class,
                        PostgresOffsetContext.class,
                        PostgresConnectorConfig.class,
                        PostgresStreamFetchTask.StoppableChangeEventSourceContext.class,
                        AtomicBoolean.class,
                        AtomicReference.class,
                        AtomicReference.class,
                        AtomicReference.class,
                        ReplicationConnection.class);
        requestRestart.setAccessible(true);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> relationAttempt =
                    executor.submit(
                            () ->
                                    (boolean)
                                            requestRestart.invoke(
                                                    null,
                                                    relationChild,
                                                    "relation",
                                                    currentOffsetContext,
                                                    connectorConfig,
                                                    new PostgresStreamFetchTask
                                                            .StoppableChangeEventSourceContext(),
                                                    refreshRequested,
                                                    restartTriggerTable,
                                                    restartTriggerSource,
                                                    frozenRestartOffset,
                                                    replicationConnection));
            Future<Boolean> pollerAttempt =
                    executor.submit(
                            () ->
                                    (boolean)
                                            requestRestart.invoke(
                                                    null,
                                                    pollerChild,
                                                    "poller",
                                                    currentOffsetContext,
                                                    connectorConfig,
                                                    new PostgresStreamFetchTask
                                                            .StoppableChangeEventSourceContext(),
                                                    refreshRequested,
                                                    restartTriggerTable,
                                                    restartTriggerSource,
                                                    frozenRestartOffset,
                                                    replicationConnection));

            Assertions.assertThat(
                            waitForRestartGateOrCompletedAttempt(
                                    relationAttempt, pollerAttempt, freezeEnterCount))
                    .isTrue();
            releaseFreeze.set(true);

            boolean relationTriggered = relationAttempt.get(5, TimeUnit.SECONDS);
            boolean pollerTriggered = pollerAttempt.get(5, TimeUnit.SECONDS);

            Assertions.assertThat((relationTriggered ? 1 : 0) + (pollerTriggered ? 1 : 0))
                    .as("only one concurrent restart attempt should win ownership")
                    .isEqualTo(1);
            Assertions.assertThat(refreshRequested).isTrue();
            Assertions.assertThat(restartTriggerTable.get()).isIn(relationChild, pollerChild);
            Assertions.assertThat(restartTriggerSource.get()).isIn("relation", "poller");
            Assertions.assertThat(frozenRestartOffset.get()).isNotNull();
            Mockito.verify(replicationConnection, Mockito.times(1)).close();
        } finally {
            releaseFreeze.set(true);
            executor.shutdownNow();
        }
    }

    @Test
    void testPollerShouldTriggerFallbackRestartAfterPublicationRefresh() {
        TableId childCa = new TableId(null, "inventory", "products_ca");
        AtomicBoolean refreshRequested = new AtomicBoolean(false);

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerFallbackRestartAfterPublicationRefresh(
                                Collections.singletonList(childCa), refreshRequested))
                .isTrue();
    }

    @Test
    void testPollerShouldNotTriggerFallbackRestartWhenNoPublishedChildrenRemain() {
        AtomicBoolean refreshRequested = new AtomicBoolean(false);

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerFallbackRestartAfterPublicationRefresh(
                                Collections.emptyList(), refreshRequested))
                .isFalse();
    }

    @Test
    void testPollerShouldNotTriggerFallbackRestartWhenRestartAlreadyRequested() {
        TableId childCa = new TableId(null, "inventory", "products_ca");
        AtomicBoolean refreshRequested = new AtomicBoolean(true);

        Assertions.assertThat(
                        Pg10StreamFetchTask.shouldTriggerFallbackRestartAfterPublicationRefresh(
                                Collections.singletonList(childCa), refreshRequested))
                .isFalse();
    }

    private static boolean waitForRestartGateOrCompletedAttempt(
            Future<Boolean> relationAttempt,
            Future<Boolean> pollerAttempt,
            AtomicInteger freezeEnterCount)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (freezeEnterCount.get() >= 2 || relationAttempt.isDone() || pollerAttempt.isDone()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }
}
