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

package org.apache.flink.cdc.connectors.postgres;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for verifying concurrency safety of PostgresVersionedTestBase utilities.
 *
 * <p>This test class verifies that the improved slot name generation and SQL parsing methods work
 * correctly under concurrent access. It includes:
 *
 * <ul>
 *   <li>Basic concurrency testing with thread-safe collections
 *   <li>Race condition detection using synchronized thread startup
 *   <li>Stress testing for uniqueness guarantees
 *   <li>SQL parsing thread safety verification
 * </ul>
 */
class PostgresConcurrencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresConcurrencyTest.class);

    @Test
    void testSlotNameGenerationConcurrency() throws Exception {
        LOG.info("Testing slot name generation under concurrent access");

        final int numThreads = 10;
        final int namesPerThread = 100;

        // Use thread-safe collection to collect results from concurrent threads
        final Set<String> allSlotNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final AtomicInteger successfulGenerations = new AtomicInteger(0);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            // Launch multiple threads to generate slot names concurrently
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                CompletableFuture<Void> future =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        List<String> threadSlotNames = new ArrayList<>();
                                        for (int j = 0; j < namesPerThread; j++) {
                                            // Generate standard slot name
                                            String slotName =
                                                    PostgresVersionedTestBase.getSlotName();
                                            threadSlotNames.add(slotName);

                                            // Generate custom prefix slot name
                                            String customSlotName =
                                                    PostgresVersionedTestBase.getSlotName("test");
                                            threadSlotNames.add(customSlotName);
                                        }

                                        // Add all generated names to the thread-safe set
                                        // This will automatically handle duplicates
                                        boolean allAdded = allSlotNames.addAll(threadSlotNames);
                                        if (allAdded || allSlotNames.size() > 0) {
                                            successfulGenerations.addAndGet(threadSlotNames.size());
                                        }

                                        LOG.debug(
                                                "Thread {} generated {} slot names",
                                                threadId,
                                                threadSlotNames.size());

                                    } catch (Exception e) {
                                        LOG.error(
                                                "Thread {} failed to generate slot names",
                                                threadId,
                                                e);
                                        throw new RuntimeException(
                                                "Slot name generation failed in thread " + threadId,
                                                e);
                                    }
                                },
                                executor);

                futures.add(future);
            }

            // Wait for all threads to complete
            for (CompletableFuture<Void> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

            // Verify uniqueness - this is the key test for concurrency safety
            int expectedTotal = numThreads * namesPerThread * 2; // 2 names per iteration
            int actualUnique = allSlotNames.size();
            int actualGenerated = successfulGenerations.get();

            LOG.info(
                    "Generated {} total names, {} unique names across {} threads",
                    actualGenerated,
                    actualUnique,
                    numThreads);

            // All names should be unique (no collisions)
            assertThat(actualUnique).isEqualTo(expectedTotal);
            assertThat(actualGenerated).isEqualTo(expectedTotal);

            // Verify naming format and constraints
            for (String slotName : allSlotNames) {
                assertThat(slotName)
                        .matches("(flink|test)_\\d+_\\d+_\\d+")
                        .hasSizeLessThanOrEqualTo(63); // PostgreSQL identifier limit
            }

            LOG.info("✓ All {} slot names are unique and follow expected format", actualUnique);

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void testSqlParsingConcurrency() throws Exception {
        LOG.info("Testing SQL parsing under concurrent access");

        final String testSql =
                "/* This is a block comment */\n"
                        + "CREATE TABLE test (id INT PRIMARY KEY);\n"
                        + "-- This is a line comment\n"
                        + "INSERT INTO test VALUES (1);\n"
                        + "/* Another\n   multiline\n   comment */\n"
                        + "SELECT * FROM test;";

        final int numThreads = 5;
        final int iterationsPerThread = 50;
        final AtomicInteger successfulParses = new AtomicInteger(0);
        final AtomicInteger totalStatements = new AtomicInteger(0);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                CompletableFuture<Void> future =
                        CompletableFuture.runAsync(
                                () -> {
                                    int threadSuccessfulParses = 0;
                                    int threadTotalStatements = 0;

                                    for (int j = 0; j < iterationsPerThread; j++) {
                                        try {
                                            List<String> statements =
                                                    PostgresVersionedTestBase.parseSqlStatements(
                                                            testSql);

                                            // Verify consistent parsing results
                                            assertThat(statements).hasSize(3);
                                            assertThat(statements.get(0))
                                                    .contains("CREATE TABLE test");
                                            assertThat(statements.get(1))
                                                    .contains("INSERT INTO test");
                                            assertThat(statements.get(2))
                                                    .contains("SELECT * FROM test");

                                            // Verify no comments remain
                                            for (String stmt : statements) {
                                                assertThat(stmt).doesNotContain("/*");
                                                assertThat(stmt).doesNotContain("*/");
                                                assertThat(stmt).doesNotContain("--");
                                            }

                                            threadSuccessfulParses++;
                                            threadTotalStatements += statements.size();

                                        } catch (Exception e) {
                                            LOG.error(
                                                    "SQL parsing failed in thread {} iteration {}",
                                                    threadId,
                                                    j,
                                                    e);
                                            throw new RuntimeException(
                                                    "SQL parsing failed in thread " + threadId, e);
                                        }
                                    }

                                    // Update global counters atomically
                                    successfulParses.addAndGet(threadSuccessfulParses);
                                    totalStatements.addAndGet(threadTotalStatements);

                                    LOG.debug(
                                            "Thread {} completed {} successful parses with {} total statements",
                                            threadId,
                                            threadSuccessfulParses,
                                            threadTotalStatements);
                                },
                                executor);

                futures.add(future);
            }

            // Wait for all threads to complete
            for (CompletableFuture<Void> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

            int expectedParses = numThreads * iterationsPerThread;
            int expectedStatements = expectedParses * 3; // 3 statements per parse

            assertThat(successfulParses.get()).isEqualTo(expectedParses);
            assertThat(totalStatements.get()).isEqualTo(expectedStatements);

            LOG.info(
                    "✓ SQL parsing completed successfully: {} parses ({} statements) across {} threads",
                    successfulParses.get(),
                    totalStatements.get(),
                    numThreads);

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void testSqlParsingWithComplexContent() {
        LOG.info("Testing SQL parsing with complex PostgreSQL content");

        final String complexSql =
                "/* Header comment */\n"
                        + "CREATE OR REPLACE FUNCTION test_func()\n"
                        + "RETURNS TEXT AS $$\n"
                        + "BEGIN\n"
                        + "    -- Function comment\n"
                        + "    RETURN 'Hello /* not a comment */ World';\n"
                        + "END;\n"
                        + "$$ LANGUAGE plpgsql;\n"
                        + "\n"
                        + "-- Table creation\n"
                        + "CREATE TABLE test_table (\n"
                        + "    id SERIAL PRIMARY KEY,\n"
                        + "    data TEXT DEFAULT '-- not a comment'\n"
                        + ");";

        List<String> statements = PostgresVersionedTestBase.parseSqlStatements(complexSql);

        assertThat(statements).hasSize(2);

        // Verify function definition is preserved correctly
        String functionDef = statements.get(0);
        assertThat(functionDef).contains("CREATE OR REPLACE FUNCTION");
        assertThat(functionDef).contains("$$ LANGUAGE plpgsql");
        assertThat(functionDef)
                .contains("'Hello /* not a comment */ World'"); // String literal preserved

        // Verify table definition
        String tableDef = statements.get(1);
        assertThat(tableDef).contains("CREATE TABLE test_table");
        assertThat(tableDef).contains("'-- not a comment'"); // String literal preserved

        LOG.info(
                "✓ Complex SQL parsing handles dollar-quoted strings and string literals correctly");
    }

    @Test
    void testEdgeCasesInSqlParsing() {
        LOG.info("Testing SQL parsing edge cases");

        // Test empty and null content
        assertThat(PostgresVersionedTestBase.parseSqlStatements(null)).isEmpty();
        assertThat(PostgresVersionedTestBase.parseSqlStatements("")).isEmpty();
        assertThat(PostgresVersionedTestBase.parseSqlStatements("   ")).isEmpty();

        // Test only comments
        assertThat(PostgresVersionedTestBase.parseSqlStatements("/* only comment */")).isEmpty();
        assertThat(PostgresVersionedTestBase.parseSqlStatements("-- only line comment")).isEmpty();

        // Test malformed content (should not crash)
        List<String> result = PostgresVersionedTestBase.parseSqlStatements("/* unclosed comment");
        assertThat(result).isNotNull(); // Should not throw exception

        LOG.info("✓ SQL parsing handles edge cases gracefully");
    }

    @Test
    void testTrueRaceConditionInSlotGeneration() throws Exception {
        LOG.info("Testing true race conditions in slot name generation");

        final int numThreads = 20;
        final int rapidGenerations = 10;

        // Use CountDownLatch to ensure all threads start simultaneously
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        final Set<String> raceConditionSlots = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final List<CompletableFuture<List<String>>> futures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            // Create threads that will all start generating slot names at exactly the same time
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                CompletableFuture<List<String>> future =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    List<String> threadSlots = new ArrayList<>();
                                    try {
                                        // Wait for all threads to be ready
                                        barrier.await(5, TimeUnit.SECONDS);

                                        // All threads start generating slot names simultaneously
                                        long startTime = System.nanoTime();
                                        for (int j = 0; j < rapidGenerations; j++) {
                                            String slot = PostgresVersionedTestBase.getSlotName();
                                            threadSlots.add(slot);

                                            // Try to detect race conditions by generating rapidly
                                            if (j % 3 == 0) {
                                                Thread.yield(); // Give other threads a chance
                                            }
                                        }
                                        long endTime = System.nanoTime();

                                        LOG.debug(
                                                "Thread {} generated {} slots in {} ms",
                                                threadId,
                                                threadSlots.size(),
                                                (endTime - startTime) / 1_000_000);

                                        // Add to shared collection (thread-safe)
                                        raceConditionSlots.addAll(threadSlots);
                                        return threadSlots;

                                    } catch (Exception e) {
                                        LOG.error(
                                                "Thread {} failed during race condition test",
                                                threadId,
                                                e);
                                        throw new RuntimeException(e);
                                    }
                                },
                                executor);

                futures.add(future);
            }

            // Start the race!
            startLatch.countDown();

            // Collect all results
            List<String> allGeneratedSlots = new ArrayList<>();
            for (CompletableFuture<List<String>> future : futures) {
                List<String> threadSlots = future.get(10, TimeUnit.SECONDS);
                allGeneratedSlots.addAll(threadSlots);
            }

            // Verify no race conditions occurred
            int expectedTotal = numThreads * rapidGenerations;
            int actualUnique = raceConditionSlots.size();
            int actualGenerated = allGeneratedSlots.size();

            LOG.info(
                    "Race condition test: {} threads generated {} slots ({} unique)",
                    numThreads,
                    actualGenerated,
                    actualUnique);

            // The critical test: all generated slot names should be unique
            assertThat(actualGenerated).isEqualTo(expectedTotal);
            assertThat(actualUnique).isEqualTo(expectedTotal); // No duplicates = no race conditions

            // Additional verification: check for any duplicate patterns
            Set<String> duplicateCheck = new HashSet<>(allGeneratedSlots);
            assertThat(duplicateCheck).hasSize(expectedTotal);

            LOG.info("✓ No race conditions detected: all {} slot names are unique", actualUnique);

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void testSlotNameUniquenessUnderStress() throws Exception {
        LOG.info("Testing slot name uniqueness under stress conditions");

        final int stressIterations = 1000;
        final Set<String> stressTestSlots = Collections.newSetFromMap(new ConcurrentHashMap<>());

        // Generate many slot names rapidly
        for (int i = 0; i < stressIterations; i++) {
            String slot1 = PostgresVersionedTestBase.getSlotName();
            String slot2 = PostgresVersionedTestBase.getSlotName("stress");

            boolean added1 = stressTestSlots.add(slot1);
            boolean added2 = stressTestSlots.add(slot2);

            // Verify each generated name is unique
            assertThat(added1)
                    .as("Slot name '%s' should be unique (iteration %d)", slot1, i)
                    .isTrue();
            assertThat(added2)
                    .as("Slot name '%s' should be unique (iteration %d)", slot2, i)
                    .isTrue();
        }

        assertThat(stressTestSlots).hasSize(stressIterations * 2);
        LOG.info(
                "✓ Generated {} unique slot names under stress conditions", stressTestSlots.size());
    }
}
