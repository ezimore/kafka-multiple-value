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
package kafka.server;

import org.apache.kafka.common.security.mla.ConsumerIdRegistryClient;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.record.mla.test.TestAdminClient;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Integration tests for consumer deletion and registry updates using a real Kafka cluster.
 *
 * <p>These tests verify that the {@link ConsumerIdRegistryClient} correctly picks up
 * changes from the Consumer ID Registry topic, including new registrations and
 * tombstone deletions, without requiring a broker restart.</p>
 *
 * <p>The tests use {@link TestAdminClient} to manage the registry topic and
 * {@link ConsumerIdRegistryClient} to read it, verifying the core registry
 * update mechanism that the MLAPlugin depends on.</p>
 *
 * <p><b>Requirements: 3.5, 3.6, 6.1</b></p>
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false")
    }
)
public class MLAConsumerRegistryIntegrationTest {

    private static final String REGISTRY_TOPIC = "_consumer_id_registry";
    private static final long POLL_TIMEOUT_MS = 15_000;
    private static final long POLL_INTERVAL_MS = 200;

    /**
     * Test: register consumer → delete consumer → consumer is removed from registry client cache.
     *
     * <p>Verifies that when a consumer is deleted from the registry topic (via tombstone),
     * the {@link ConsumerIdRegistryClient} picks up the deletion and removes the consumer
     * from its in-memory cache. This validates that retired consumer IDs are properly
     * handled by the registry client.</p>
     *
     * <p><b>Validates: Requirements 3.5, 3.6, 6.1</b></p>
     */
    @ClusterTest
    public void testDeletedConsumerRemovedFromRegistryClient(ClusterInstance cluster) throws Exception {
        String bootstrapServers = cluster.bootstrapServers();

        try (TestAdminClient adminClient = new TestAdminClient(bootstrapServers)) {
            // Step 1: Create the registry topic and register two consumers
            adminClient.createRegistryTopic(REGISTRY_TOPIC, 1);
            int c0Id = adminClient.registerConsumer("User:c0");
            int c1Id = adminClient.registerConsumer("User:c1");
            assertEquals(0, c0Id, "First consumer should get ID 0");
            assertEquals(1, c1Id, "Second consumer should get ID 1");

            // Step 2: Start a ConsumerIdRegistryClient and wait for it to pick up both registrations
            try (ConsumerIdRegistryClient registryClient = new ConsumerIdRegistryClient(bootstrapServers, REGISTRY_TOPIC)) {
                registryClient.start();

                // Wait for both consumers to appear in the cache
                waitForCondition(
                    () -> registryClient.getConsumerId("User:c0") != null
                        && registryClient.getConsumerId("User:c1") != null,
                    POLL_TIMEOUT_MS,
                    "Both consumers should be in the registry client cache"
                );

                assertEquals(Integer.valueOf(0), registryClient.getConsumerId("User:c0"));
                assertEquals(Integer.valueOf(1), registryClient.getConsumerId("User:c1"));
                assertEquals(2, registryClient.getAllMappings().size(),
                    "Registry client should have exactly 2 mappings");

                // Step 3: Delete c1 from the registry
                adminClient.deleteConsumer("User:c1");

                // Step 4: Wait for the registry client to pick up the tombstone
                waitForCondition(
                    () -> registryClient.getConsumerId("User:c1") == null,
                    POLL_TIMEOUT_MS,
                    "Deleted consumer c1 should be removed from registry client cache"
                );

                // Step 5: Verify c0 is still present and c1 is gone
                assertNotNull(registryClient.getConsumerId("User:c0"),
                    "Consumer c0 should still be in the cache");
                assertEquals(Integer.valueOf(0), registryClient.getConsumerId("User:c0"));
                assertNull(registryClient.getConsumerId("User:c1"),
                    "Deleted consumer c1 should not be in the cache");
                assertEquals(1, registryClient.getAllMappings().size(),
                    "Registry client should have exactly 1 mapping after deletion");

                // Step 6: Verify the retired ID is tracked by the admin client
                assertEquals(1, adminClient.getRetiredIds().size());
                assertEquals(true, adminClient.getRetiredIds().contains(1),
                    "Consumer ID 1 should be in the retired set");
            }
        }
    }

    /**
     * Test: MLAPlugin (via ConsumerIdRegistryClient) picks up new consumer registrations
     * without broker restart.
     *
     * <p>Verifies that when a new consumer is registered in the registry topic after
     * the {@link ConsumerIdRegistryClient} has started, the client dynamically picks
     * up the new registration without needing to be restarted.</p>
     *
     * <p><b>Validates: Requirements 3.5, 6.1</b></p>
     */
    @ClusterTest
    public void testNewRegistrationPickedUpWithoutRestart(ClusterInstance cluster) throws Exception {
        String bootstrapServers = cluster.bootstrapServers();

        try (TestAdminClient adminClient = new TestAdminClient(bootstrapServers)) {
            // Step 1: Create the registry topic and register initial consumer c0
            adminClient.createRegistryTopic(REGISTRY_TOPIC, 1);
            int c0Id = adminClient.registerConsumer("User:c0");
            assertEquals(0, c0Id);

            // Step 2: Start the registry client and wait for it to see c0
            try (ConsumerIdRegistryClient registryClient = new ConsumerIdRegistryClient(bootstrapServers, REGISTRY_TOPIC)) {
                registryClient.start();

                waitForCondition(
                    () -> registryClient.getConsumerId("User:c0") != null,
                    POLL_TIMEOUT_MS,
                    "Consumer c0 should be in the registry client cache"
                );
                assertEquals(Integer.valueOf(0), registryClient.getConsumerId("User:c0"));

                // Verify c2 is NOT yet in the cache
                assertNull(registryClient.getConsumerId("User:c2"),
                    "Consumer c2 should not be in the cache yet");

                // Step 3: Register c2 AFTER the registry client has started
                int c2Id = adminClient.registerConsumer("User:c2");
                assertEquals(1, c2Id, "Second consumer should get ID 1");

                // Step 4: Wait for the registry client to pick up c2 dynamically
                waitForCondition(
                    () -> registryClient.getConsumerId("User:c2") != null,
                    POLL_TIMEOUT_MS,
                    "Newly registered consumer c2 should be picked up by registry client"
                );

                // Step 5: Verify both consumers are in the cache
                assertEquals(Integer.valueOf(0), registryClient.getConsumerId("User:c0"));
                assertEquals(Integer.valueOf(1), registryClient.getConsumerId("User:c2"));
                Map<String, Integer> allMappings = registryClient.getAllMappings();
                assertEquals(2, allMappings.size(),
                    "Registry client should have exactly 2 mappings");
                assertEquals(Integer.valueOf(0), allMappings.get("User:c0"));
                assertEquals(Integer.valueOf(1), allMappings.get("User:c2"));
            }
        }
    }

    /**
     * Waits for a condition to become true within the specified timeout.
     *
     * @param condition   the condition to check
     * @param timeoutMs   the maximum time to wait in milliseconds
     * @param description a description of the condition for error messages
     * @throws AssertionError if the condition is not met within the timeout
     */
    private void waitForCondition(BooleanSupplier condition, long timeoutMs, String description)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new AssertionError("Timed out waiting for condition: " + description);
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
