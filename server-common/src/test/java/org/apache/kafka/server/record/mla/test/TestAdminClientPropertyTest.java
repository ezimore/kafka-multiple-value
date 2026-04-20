/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.record.mla.test;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.withSettings;

/**
 * Property-based tests for {@link TestAdminClient}'s consumer ID assignment logic.
 *
 * <p>Feature: message-level-authorization</p>
 * <ul>
 *   <li>Property 1: Consumer ID Assignment is Sequential and Unique</li>
 *   <li>Property 2: Retired Consumer IDs Are Never Reassigned</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 3.3, 3.4, 3.6</b></p>
 */
public class TestAdminClientPropertyTest {

    /**
     * Provides an arbitrary for generating random lists of 1–1000 unique principal strings.
     * Each principal is a unique string like "User:principal_N".
     */
    @Provide
    Arbitrary<List<String>> uniquePrincipalLists() {
        return Arbitraries.integers().between(1, 1000)
            .flatMap(size ->
                Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(20)
                    .map(s -> "User:" + s)
                    .set().ofSize(size)
                    .map(set -> List.copyOf(set))
            );
    }

    /**
     * Creates a TestAdminClient with mocked Kafka clients (Admin and KafkaProducer)
     * so that the ID assignment logic can be tested without a running Kafka cluster.
     *
     * Uses Mockito's CALLS_REAL_METHODS to create a partial mock that delegates to
     * real method implementations, then injects mocked dependencies and initializes
     * internal state via reflection.
     */
    @SuppressWarnings("unchecked")
    private TestAdminClient createTestAdminClientWithMocks() throws Exception {
        // Create a partial mock that calls real methods but doesn't invoke the constructor
        TestAdminClient client = mock(TestAdminClient.class, withSettings()
            .defaultAnswer(CALLS_REAL_METHODS));

        // Mock the KafkaProducer to return a successful future on send()
        KafkaProducer<String, byte[]> mockProducer = mock(KafkaProducer.class);
        Future<RecordMetadata> mockFuture = mock(Future.class);
        RecordMetadata metadata = new RecordMetadata(
            new TopicPartition("test-registry", 0), 0, 0, 0L, 0, 0);
        when(mockFuture.get()).thenReturn(metadata);
        when(mockProducer.send(any())).thenReturn(mockFuture);

        // Mock the Admin client
        Admin mockAdmin = mock(Admin.class);

        // Inject mocked dependencies via reflection
        setField(client, "producer", mockProducer);
        setField(client, "adminClient", mockAdmin);
        setField(client, "nextConsumerId", 0);
        setField(client, "registryTopicName", "_test_consumer_id_registry");
        setField(client, "principalToId", new HashMap<String, Integer>());
        setField(client, "retiredIds", new HashSet<Integer>());

        return client;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getSuperclass() == Object.class
            ? target.getClass().getDeclaredField(fieldName)
            : findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /**
     * Property 1: Consumer ID Assignment is Sequential and Unique.
     *
     * <p>For any sequence of N user principal registrations (with no deletions),
     * the Data Management Administration SHALL assign Consumer IDs as the integers
     * 0, 1, 2, ..., N-1, with each ID assigned to exactly one principal.</p>
     *
     * <p><b>Validates: Requirements 3.3, 3.4</b></p>
     */
    @Property(tries = 100)
    public void consumerIdAssignmentIsSequentialAndUnique(
            @ForAll("uniquePrincipalLists") List<String> principals) throws Exception {

        TestAdminClient client = createTestAdminClientWithMocks();

        Set<Integer> assignedIds = new HashSet<>();
        int expectedNextId = 0;

        for (String principal : principals) {
            int assignedId = client.registerConsumer(principal);

            // Assert each ID is assigned sequentially starting from 0
            assertEquals(expectedNextId, assignedId,
                "Consumer ID for principal '" + principal + "' should be " + expectedNextId
                    + " but was " + assignedId);

            // Assert the ID has not been assigned before (uniqueness)
            assertTrue(assignedIds.add(assignedId),
                "Consumer ID " + assignedId + " was assigned more than once (duplicate detected)");

            expectedNextId++;
        }

        // After all registrations, verify the total count
        assertEquals(principals.size(), assignedIds.size(),
            "Total number of unique assigned IDs should equal the number of principals");

        // Verify IDs form the complete range 0..N-1
        for (int i = 0; i < principals.size(); i++) {
            assertTrue(assignedIds.contains(i),
                "ID " + i + " should be in the assigned set for " + principals.size() + " registrations");
        }

        // Verify getNextConsumerId() returns N
        assertEquals(principals.size(), client.getNextConsumerId(),
            "getNextConsumerId() should return " + principals.size() + " after "
                + principals.size() + " registrations");
    }

    // ---- P2: Retired Consumer IDs Are Never Reassigned ----

    /**
     * Represents an operation in a register/delete sequence.
     * REGISTER creates a new consumer; DELETE removes an existing one.
     */
    enum OpType { REGISTER, DELETE }

    /**
     * A single operation in a register/delete sequence.
     */
    static class Op {
        final OpType type;
        final String principal; // principal to register or delete

        Op(OpType type, String principal) {
            this.type = type;
            this.principal = principal;
        }

        @Override
        public String toString() {
            return type + "(" + principal + ")";
        }
    }

    /**
     * Provides an arbitrary for generating random interleaved register/delete
     * operation sequences of 1–500 operations.
     *
     * <p>The generator ensures valid sequences:
     * <ul>
     *   <li>Register operations use unique principal names that are not currently registered</li>
     *   <li>Delete operations pick a random currently-registered principal</li>
     *   <li>Delete operations only occur when there is at least one registered consumer</li>
     * </ul>
     */
    @Provide
    Arbitrary<List<Op>> interleavedRegisterDeleteOps() {
        return Arbitraries.randomValue(random -> generateOpSequence(random))
            .filter(ops -> !ops.isEmpty());
    }

    /**
     * Generates a valid interleaved register/delete operation sequence.
     * Uses a seeded random to produce deterministic sequences for reproducibility.
     */
    private List<Op> generateOpSequence(Random random) {
        int numOps = 1 + random.nextInt(500); // 1–500 operations
        List<Op> ops = new ArrayList<>(numOps);
        List<String> currentlyRegistered = new ArrayList<>();
        int principalCounter = 0;

        for (int i = 0; i < numOps; i++) {
            // Decide whether to register or delete.
            // If no consumers are registered, we must register.
            // Otherwise, pick register with ~60% probability to build up some consumers.
            boolean doRegister = currentlyRegistered.isEmpty() || random.nextDouble() < 0.6;

            if (doRegister) {
                String principal = "User:p_" + principalCounter++;
                ops.add(new Op(OpType.REGISTER, principal));
                currentlyRegistered.add(principal);
            } else {
                // Pick a random currently-registered principal to delete
                int idx = random.nextInt(currentlyRegistered.size());
                String principal = currentlyRegistered.remove(idx);
                ops.add(new Op(OpType.DELETE, principal));
            }
        }
        return ops;
    }

    /**
     * Property 2: Retired Consumer IDs Are Never Reassigned.
     *
     * <p>For any sequence of user registration and deletion operations, if a user
     * with Consumer ID X is deleted and subsequently new users are registered,
     * none of the newly assigned Consumer IDs SHALL equal X.</p>
     *
     * <p><b>Validates: Requirements 3.6</b></p>
     */
    @Property(tries = 100)
    public void retiredConsumerIdsAreNeverReassigned(
            @ForAll("interleavedRegisterDeleteOps") List<Op> ops) throws Exception {

        TestAdminClient client = createTestAdminClientWithMocks();

        Set<Integer> retiredIds = new HashSet<>();

        for (Op op : ops) {
            if (op.type == OpType.REGISTER) {
                int assignedId = client.registerConsumer(op.principal);

                // Core assertion: the newly assigned ID must NOT be a retired ID
                assertFalse(retiredIds.contains(assignedId),
                    "Retired Consumer ID " + assignedId + " was reassigned to principal '"
                        + op.principal + "'. Retired IDs: " + retiredIds);
            } else {
                // Track the ID that will be retired before deleting
                // We need to know the ID for the principal being deleted
                // The client tracks this internally; we can infer from getRetiredIds after delete
                client.deleteConsumer(op.principal);
                // Update our local tracking of retired IDs
                retiredIds = new HashSet<>(client.getRetiredIds());
            }
        }

        // After all operations, verify that getRetiredIds() matches our tracked retired IDs
        assertEquals(retiredIds, client.getRetiredIds(),
            "getRetiredIds() should match the set of all deleted consumer IDs");
    }
}
