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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.security.mla.AuthorizationBitmap;
import org.apache.kafka.common.security.mla.ConsumerIdRegistryClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.record.mla.MLAPlugin;
import org.apache.kafka.server.record.mla.test.TestAdminClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for Message-Level Authorization (MLA) with selective
 * authorization across multiple consumers.
 *
 * <p>This test exercises the complete MLA pipeline:</p>
 * <ol>
 *   <li>{@link TestAdminClient} creates the Consumer ID Registry topic and registers consumers</li>
 *   <li>Records are produced with authorization bitmaps built via {@link AuthorizationBitmap}</li>
 *   <li>The broker's {@link MLAPlugin} filters records during fetch based on the authorization bitmaps</li>
 *   <li>Consumers receive only their authorized records</li>
 * </ol>
 *
 * <p><b>Test environment constraint:</b> In a non-secured test cluster, all consumers
 * authenticate as {@code User:ANONYMOUS}. The test registers {@code User:ANONYMOUS}
 * as one of the consumers (c0) and verifies that the MLAPlugin correctly filters
 * records based on the authorization bitmap and the consumer's bitmask. Additional
 * consumers (c1–c4) are registered to build realistic multi-consumer bitmaps.</p>
 *
 * <p>The MLAPlugin is loaded programmatically after the broker starts (rather than
 * via broker config) because the plugin needs {@code bootstrap.servers} to connect
 * to the Consumer ID Registry topic, and the broker's port is not known at
 * annotation time in the {@code @ClusterTest} framework.</p>
 *
 * <p><b>Requirements: 5.1, 5.6, 7.1, 7.2, 7.3, 7.4</b></p>
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false")
    }
)
public class MLAEndToEndTest {

    private static final String REGISTRY_TOPIC = "_consumer_id_registry";
    private static final String MLA_AUTHZ_HEADER_KEY = "mla-authz-bitmap";
    private static final long POLL_TIMEOUT_MS = 15_000;
    private static final long POLL_INTERVAL_MS = 200;

    // In a non-secured cluster, all consumers are User:ANONYMOUS
    private static final String ANONYMOUS_PRINCIPAL = "User:ANONYMOUS";

    /**
     * E2E Scenario 1: Selective authorization with multiple consumers.
     * Header stripping ENABLED (default).
     *
     * <p>Registers 5 consumers (c0=User:ANONYMOUS, c1–c4), creates an MLA-enabled topic,
     * produces 4 records with varying authorization sets, and verifies that the consumer
     * (User:ANONYMOUS = c0) receives exactly the records it is authorized for.</p>
     *
     * <p>When header stripping is enabled, received records must NOT contain the
     * {@code mla-authz-bitmap} header.</p>
     *
     * <p><b>Validates: Requirements 5.1, 5.6, 7.1, 7.2, 7.3</b></p>
     */
    @ClusterTest
    public void testSelectiveAuthorizationWithStripEnabled(ClusterInstance cluster) throws Exception {
        runSelectiveAuthorizationTest(cluster, true);
    }

    /**
     * E2E Scenario 1: Selective authorization with multiple consumers.
     * Header stripping DISABLED.
     *
     * <p>Same scenario as {@link #testSelectiveAuthorizationWithStripEnabled} but with
     * header stripping disabled. Received records must contain the original
     * {@code mla-authz-bitmap} header.</p>
     *
     * <p><b>Validates: Requirements 5.1, 5.6, 7.1, 7.2, 7.4</b></p>
     */
    @ClusterTest
    public void testSelectiveAuthorizationWithStripDisabled(ClusterInstance cluster) throws Exception {
        runSelectiveAuthorizationTest(cluster, false);
    }

    /**
     * Core test logic for selective authorization with multiple consumers.
     *
     * @param cluster      the test cluster instance
     * @param stripEnabled whether header stripping is enabled
     */
    private void runSelectiveAuthorizationTest(ClusterInstance cluster, boolean stripEnabled) throws Exception {
        String bootstrapServers = cluster.bootstrapServers();
        String topic = "mla-e2e-topic-" + (stripEnabled ? "strip" : "nostrip") + "-" + System.nanoTime();

        // Step 1: Create the registry topic and register 5 consumers
        try (TestAdminClient adminClient = new TestAdminClient(bootstrapServers)) {
            adminClient.createRegistryTopic(REGISTRY_TOPIC, 1);

            // c0 = User:ANONYMOUS (the principal all test consumers will have)
            int c0Id = adminClient.registerConsumer(ANONYMOUS_PRINCIPAL);
            int c1Id = adminClient.registerConsumer("User:c1");
            int c2Id = adminClient.registerConsumer("User:c2");
            int c3Id = adminClient.registerConsumer("User:c3");
            int c4Id = adminClient.registerConsumer("User:c4");

            assertEquals(0, c0Id, "User:ANONYMOUS should get consumer ID 0");
            assertEquals(1, c1Id);
            assertEquals(2, c2Id);
            assertEquals(3, c3Id);
            assertEquals(4, c4Id);

            // Step 2: Programmatically load the MLAPlugin into the broker
            MLAPlugin mlaPlugin = loadMLAPlugin(cluster, bootstrapServers, stripEnabled);

            // Step 3: Wait for the MLAPlugin's registry client to pick up the consumer registrations
            waitForRegistryReady(mlaPlugin);

            // Step 4: Create the MLA-enabled topic
            cluster.createTopic(topic, 1, (short) 1,
                Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG,
                       "org.apache.kafka.server.record.mla.MLAPlugin"));

            // Step 5: Produce 4 records with varying authorization sets
            Map<String, byte[]> recordBitmaps = new HashMap<>();

            // R1: "only-c0" — authorized for {c0} only
            recordBitmaps.put("only-c0", AuthorizationBitmap.create(Set.of(c0Id)));

            // R2: "c0-and-c2" — authorized for {c0, c2}
            recordBitmaps.put("c0-and-c2", AuthorizationBitmap.create(Set.of(c0Id, c2Id)));

            // R3: "all" — authorized for {c0, c1, c2, c3, c4}
            recordBitmaps.put("all", AuthorizationBitmap.create(Set.of(c0Id, c1Id, c2Id, c3Id, c4Id)));

            // R4: "none" — authorized for {} (empty set = zero-length bitmap)
            recordBitmaps.put("none", AuthorizationBitmap.create(Set.of()));

            try (KafkaProducer<String, String> producer = createProducer(cluster)) {
                for (Map.Entry<String, byte[]> entry : List.of(
                        Map.entry("only-c0", recordBitmaps.get("only-c0")),
                        Map.entry("c0-and-c2", recordBitmaps.get("c0-and-c2")),
                        Map.entry("all", recordBitmaps.get("all")),
                        Map.entry("none", recordBitmaps.get("none")))) {
                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, entry.getKey(), "value-" + entry.getKey());
                    record.headers().add(new RecordHeader(MLA_AUTHZ_HEADER_KEY, entry.getValue()));
                    producer.send(record).get();
                }
            }

            // Step 6: Consume as User:ANONYMOUS (c0) and verify filtering
            // c0 should receive: R1 ("only-c0"), R2 ("c0-and-c2"), R3 ("all")
            // c0 should NOT receive: R4 ("none")
            List<ConsumerRecord<String, String>> receivedRecords;
            try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
                consumer.subscribe(List.of(topic));
                receivedRecords = pollUntilCount(consumer, 3, 30_000);
            }

            // Verify exactly 3 records received
            assertEquals(3, receivedRecords.size(),
                "User:ANONYMOUS (c0) should receive exactly 3 authorized records");

            // Verify the correct records were received
            Set<String> receivedKeys = new HashSet<>();
            for (ConsumerRecord<String, String> record : receivedRecords) {
                receivedKeys.add(record.key());
            }
            assertTrue(receivedKeys.contains("only-c0"),
                "c0 should receive R1 (only-c0)");
            assertTrue(receivedKeys.contains("c0-and-c2"),
                "c0 should receive R2 (c0-and-c2)");
            assertTrue(receivedKeys.contains("all"),
                "c0 should receive R3 (all)");
            assertFalse(receivedKeys.contains("none"),
                "c0 should NOT receive R4 (none)");

            // Verify header stripping behavior
            for (ConsumerRecord<String, String> record : receivedRecords) {
                boolean hasAuthzHeader = false;
                for (Header header : record.headers()) {
                    if (MLA_AUTHZ_HEADER_KEY.equals(header.key())) {
                        hasAuthzHeader = true;
                        break;
                    }
                }

                if (stripEnabled) {
                    assertFalse(hasAuthzHeader,
                        "With strip enabled, record '" + record.key()
                            + "' should NOT have the mla-authz-bitmap header");
                } else {
                    assertTrue(hasAuthzHeader,
                        "With strip disabled, record '" + record.key()
                            + "' should have the mla-authz-bitmap header");
                }
            }

            // Verify record values are preserved
            for (ConsumerRecord<String, String> record : receivedRecords) {
                assertEquals("value-" + record.key(), record.value(),
                    "Record value should be preserved for key '" + record.key() + "'");
            }

            // Cleanup: close the MLAPlugin
            mlaPlugin.close();
        }
    }

    /**
     * E2E Scenario 2: Consumer registration after record production.
     *
     * <p>Verifies that a record produced with a pre-set bitmap bit for a not-yet-registered
     * consumer ID can be received by the consumer once it is registered. The bitmap is
     * constructed with IDs {0, 2, 5} where ID 5 has no registered consumer at production
     * time.</p>
     *
     * <p><b>Test environment constraint:</b> In a non-secured test cluster, all consumers
     * authenticate as {@code User:ANONYMOUS}. The test registers {@code User:ANONYMOUS}
     * as c0 (ID 0) and verifies:</p>
     * <ol>
     *   <li>c0 (User:ANONYMOUS, ID 0) receives the record before c5 is registered</li>
     *   <li>The authorization bitmap correctly has bit 5 set for the future consumer</li>
     *   <li>After registering consumers up to ID 5 (User:c5), the bitmap still authorizes
     *       the pre-existing consumer c0</li>
     *   <li>A new consumer (still User:ANONYMOUS = c0) seeking to the beginning still
     *       receives the record after c5 registration</li>
     * </ol>
     *
     * <p><b>Validates: Requirements 5.1, 7.2</b></p>
     */
    @ClusterTest
    public void testConsumerRegistrationAfterRecordProduction(ClusterInstance cluster) throws Exception {
        String bootstrapServers = cluster.bootstrapServers();
        String topic = "mla-e2e-late-reg-" + System.nanoTime();

        try (TestAdminClient adminClient = new TestAdminClient(bootstrapServers)) {
            // Step 1: Create the registry topic and register 3 consumers
            adminClient.createRegistryTopic(REGISTRY_TOPIC, 1);

            // c0 = User:ANONYMOUS (ID 0), c1 (ID 1), c2 (ID 2)
            int c0Id = adminClient.registerConsumer(ANONYMOUS_PRINCIPAL);
            int c1Id = adminClient.registerConsumer("User:c1");
            int c2Id = adminClient.registerConsumer("User:c2");

            assertEquals(0, c0Id, "User:ANONYMOUS should get consumer ID 0");
            assertEquals(1, c1Id);
            assertEquals(2, c2Id);

            // Step 2: Programmatically load the MLAPlugin (strip enabled by default)
            MLAPlugin mlaPlugin = loadMLAPlugin(cluster, bootstrapServers, true);

            // Step 3: Wait for the MLAPlugin's registry client to pick up the initial registrations
            waitForRegistryReady(mlaPlugin);

            // Step 4: Create the MLA-enabled topic
            cluster.createTopic(topic, 1, (short) 1,
                Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG,
                       "org.apache.kafka.server.record.mla.MLAPlugin"));

            // Step 5: Produce a record authorized for {c0, c2, c5}
            // c5 (ID 5) is NOT yet registered, but we pre-set bit 5 in the bitmap.
            // We use AuthorizationBitmap.create() directly with raw IDs since c5
            // doesn't exist in the registry yet.
            byte[] bitmap = AuthorizationBitmap.create(Set.of(c0Id, c2Id, 5));

            // Verify the bitmap has bit 5 set (the key insight of this test)
            assertTrue(AuthorizationBitmap.getBit(bitmap, 0),
                "Bitmap should have bit 0 set (c0)");
            assertTrue(AuthorizationBitmap.getBit(bitmap, 2),
                "Bitmap should have bit 2 set (c2)");
            assertTrue(AuthorizationBitmap.getBit(bitmap, 5),
                "Bitmap should have bit 5 set (future c5)");
            assertFalse(AuthorizationBitmap.getBit(bitmap, 1),
                "Bitmap should NOT have bit 1 set (c1 not authorized)");

            try (KafkaProducer<String, String> producer = createProducer(cluster)) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, "late-reg-record", "value-late-reg");
                record.headers().add(new RecordHeader(MLA_AUTHZ_HEADER_KEY, bitmap));
                producer.send(record).get();
            }

            // Step 6: Verify c0 (User:ANONYMOUS, ID 0) receives the record
            // c0 is authorized because bit 0 is set in the bitmap
            List<ConsumerRecord<String, String>> receivedRecords;
            try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
                consumer.subscribe(List.of(topic));
                receivedRecords = pollUntilCount(consumer, 1, 30_000);
            }

            assertEquals(1, receivedRecords.size(),
                "User:ANONYMOUS (c0) should receive the record (bit 0 is set)");
            assertEquals("late-reg-record", receivedRecords.get(0).key());
            assertEquals("value-late-reg", receivedRecords.get(0).value());

            // Step 7: Register consumers c3, c4, c5 to fill IDs up to 5
            int c3Id = adminClient.registerConsumer("User:c3");
            int c4Id = adminClient.registerConsumer("User:c4");
            int c5Id = adminClient.registerConsumer("User:c5");

            assertEquals(3, c3Id);
            assertEquals(4, c4Id);
            assertEquals(5, c5Id, "User:c5 should get consumer ID 5");

            // Step 8: Wait for the MLAPlugin to pick up the new c5 registration
            waitForRegistryPrincipal(mlaPlugin, "User:c5");

            // Step 9: Verify that a new consumer (still User:ANONYMOUS = c0) seeking
            // to the beginning still receives the record. This confirms the bitmap
            // with pre-set bit 5 remains valid and c0 is still authorized.
            List<ConsumerRecord<String, String>> replayedRecords;
            try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
                consumer.subscribe(List.of(topic));
                replayedRecords = pollUntilCount(consumer, 1, 30_000);
            }

            assertEquals(1, replayedRecords.size(),
                "After c5 registration, c0 should still receive the record on replay");
            assertEquals("late-reg-record", replayedRecords.get(0).key());
            assertEquals("value-late-reg", replayedRecords.get(0).value());

            // Cleanup
            mlaPlugin.close();
        }
    }

    /**
     * E2E Scenario 3: Consumer deletion stops delivery.
     *
     * <p>Verifies that after a consumer principal is deleted from the Consumer ID Registry,
     * the MLAPlugin stops delivering records to that consumer even if the authorization
     * bitmap still includes the deleted consumer's bit position.</p>
     *
     * <p><b>Test environment constraint:</b> In a non-secured test cluster, all consumers
     * authenticate as {@code User:ANONYMOUS}. The test registers {@code User:ANONYMOUS}
     * as c0 (ID 0), then deletes it. After deletion, the MLAPlugin cannot generate a
     * bitmask for the deleted principal, so all records are filtered out.</p>
     *
     * <p><b>Test sequence:</b></p>
     * <ol>
     *   <li>Register 3 consumers: User:ANONYMOUS (c0, ID 0), User:c1 (ID 1), User:c2 (ID 2)</li>
     *   <li>Produce R1 authorized for {c0, c1}</li>
     *   <li>Verify User:ANONYMOUS (c0) receives R1</li>
     *   <li>Delete User:ANONYMOUS (c0) from the registry</li>
     *   <li>Wait for MLAPlugin to pick up the deletion</li>
     *   <li>Produce R2 authorized for {c0, c1} (bitmap still has bit 0 set)</li>
     *   <li>Verify User:ANONYMOUS does NOT receive R2</li>
     * </ol>
     *
     * <p><b>Validates: Requirements 3.5, 3.6, 7.2</b></p>
     */
    @ClusterTest
    public void testConsumerDeletionStopsDelivery(ClusterInstance cluster) throws Exception {
        String bootstrapServers = cluster.bootstrapServers();
        String topic = "mla-e2e-deletion-" + System.nanoTime();

        try (TestAdminClient adminClient = new TestAdminClient(bootstrapServers)) {
            // Step 1: Create the registry topic and register 3 consumers
            adminClient.createRegistryTopic(REGISTRY_TOPIC, 1);

            int c0Id = adminClient.registerConsumer(ANONYMOUS_PRINCIPAL); // ID 0
            int c1Id = adminClient.registerConsumer("User:c1");           // ID 1
            int c2Id = adminClient.registerConsumer("User:c2");           // ID 2

            assertEquals(0, c0Id, "User:ANONYMOUS should get consumer ID 0");
            assertEquals(1, c1Id);
            assertEquals(2, c2Id);

            // Step 2: Load the MLAPlugin (strip enabled by default)
            MLAPlugin mlaPlugin = loadMLAPlugin(cluster, bootstrapServers, true);

            // Step 3: Wait for the MLAPlugin to pick up all registrations
            waitForRegistryReady(mlaPlugin);

            // Step 4: Create the MLA-enabled topic
            cluster.createTopic(topic, 1, (short) 1,
                Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG,
                       "org.apache.kafka.server.record.mla.MLAPlugin"));

            // Step 5: Produce R1 authorized for {c0, c1}
            byte[] bitmapR1 = AuthorizationBitmap.create(Set.of(c0Id, c1Id));
            try (KafkaProducer<String, String> producer = createProducer(cluster)) {
                ProducerRecord<String, String> r1 =
                    new ProducerRecord<>(topic, "r1", "value-r1");
                r1.headers().add(new RecordHeader(MLA_AUTHZ_HEADER_KEY, bitmapR1));
                producer.send(r1).get();
            }

            // Step 6: Verify User:ANONYMOUS (c0) receives R1
            List<ConsumerRecord<String, String>> receivedR1;
            try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
                consumer.subscribe(List.of(topic));
                receivedR1 = pollUntilCount(consumer, 1, 30_000);
            }

            assertEquals(1, receivedR1.size(),
                "User:ANONYMOUS (c0) should receive R1 before deletion");
            assertEquals("r1", receivedR1.get(0).key());
            assertEquals("value-r1", receivedR1.get(0).value());

            // Step 7: Delete User:ANONYMOUS (c0) from the registry
            adminClient.deleteConsumer(ANONYMOUS_PRINCIPAL);

            // Step 8: Wait for the MLAPlugin to pick up the deletion
            waitForRegistryPrincipalRemoved(mlaPlugin, ANONYMOUS_PRINCIPAL);

            // Step 9: Produce R2 authorized for {c0, c1} (bitmap still has bit 0 set)
            // Even though c0 is deleted, the bitmap is constructed with the same IDs.
            byte[] bitmapR2 = AuthorizationBitmap.create(Set.of(c0Id, c1Id));
            try (KafkaProducer<String, String> producer = createProducer(cluster)) {
                ProducerRecord<String, String> r2 =
                    new ProducerRecord<>(topic, "r2", "value-r2");
                r2.headers().add(new RecordHeader(MLA_AUTHZ_HEADER_KEY, bitmapR2));
                producer.send(r2).get();
            }

            // Step 10: Verify User:ANONYMOUS does NOT receive R2
            // After deletion, the MLAPlugin can't generate a bitmask for User:ANONYMOUS
            // because the principal is no longer in the registry. All records are filtered.
            // Use a new consumer group to start fresh (auto.offset.reset=earliest will
            // read from the beginning, but R1 was already consumed by the previous group).
            // We use a fresh consumer that reads from earliest — it will see both R1 and R2
            // in the log, but the MLAPlugin should filter BOTH because User:ANONYMOUS is
            // no longer in the registry.
            List<ConsumerRecord<String, String>> receivedAfterDeletion;
            try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
                consumer.subscribe(List.of(topic));
                // Poll for a reasonable time — we expect 0 records
                receivedAfterDeletion = pollUntilCount(consumer, 1, 10_000);
            }

            assertTrue(receivedAfterDeletion.isEmpty(),
                "User:ANONYMOUS should NOT receive any records after deletion, but received: "
                    + receivedAfterDeletion.size() + " record(s)");

            // Cleanup
            mlaPlugin.close();
        }
    }

    /**
     * E2E Scenario 4: Non-MLA topic passthrough.
     *
     * <p>Verifies that topics without the {@code record.fetch.plugins} configuration
     * are completely unaffected by the MLA feature. Records produced without
     * authorization headers are delivered to all consumers without any filtering.</p>
     *
     * <p><b>Test sequence:</b></p>
     * <ol>
     *   <li>Create a regular topic (no {@code record.fetch.plugins} config)</li>
     *   <li>Produce 5 records with plain key/value (no authorization headers)</li>
     *   <li>Consume and verify all 5 records are received</li>
     *   <li>Verify record content (keys and values) is preserved</li>
     * </ol>
     *
     * <p><b>Validates: Requirements 9.1, 9.2</b></p>
     */
    @ClusterTest
    public void testNonMLATopicPassthrough(ClusterInstance cluster) throws Exception {
        String topic = "non-mla-passthrough-" + System.nanoTime();
        int recordCount = 5;

        // Step 1: Create a regular topic WITHOUT record.fetch.plugins config
        cluster.createTopic(topic, 1, (short) 1);

        // Step 2: Produce 5 records with plain key/value (no authorization headers)
        try (KafkaProducer<String, String> producer = createProducer(cluster)) {
            for (int i = 0; i < recordCount; i++) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, "key-" + i, "value-" + i);
                producer.send(record).get();
            }
        }

        // Step 3: Consume and verify all 5 records are received
        List<ConsumerRecord<String, String>> receivedRecords;
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster)) {
            consumer.subscribe(List.of(topic));
            receivedRecords = pollUntilCount(consumer, recordCount, 30_000);
        }

        assertEquals(recordCount, receivedRecords.size(),
            "All " + recordCount + " records should be received on a non-MLA topic");

        // Step 4: Verify record content (keys and values) is preserved
        for (int i = 0; i < recordCount; i++) {
            ConsumerRecord<String, String> record = receivedRecords.get(i);
            assertEquals("key-" + i, record.key(),
                "Record key should be preserved for record " + i);
            assertEquals("value-" + i, record.value(),
                "Record value should be preserved for record " + i);
        }
    }

    /**
     * Programmatically load and configure the MLAPlugin into the broker.
     *
     * <p>The MLAPlugin cannot be loaded via broker config in the {@code @ClusterTest}
     * framework because it needs {@code bootstrap.servers} to connect to the Consumer
     * ID Registry topic, and the broker's port is not known at annotation time.
     * Instead, we create the plugin after the broker starts, configure it with the
     * actual bootstrap servers, and add it to the broker's plugin list.</p>
     *
     * @param cluster          the test cluster instance
     * @param bootstrapServers the actual bootstrap servers address
     * @param stripEnabled     whether header stripping is enabled
     * @return the configured and started MLAPlugin
     */
    @SuppressWarnings("unchecked")
    private MLAPlugin loadMLAPlugin(ClusterInstance cluster, String bootstrapServers,
                                    boolean stripEnabled) throws Exception {
        MLAPlugin plugin = new MLAPlugin();

        // Configure with the actual bootstrap servers
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put(MLAPlugin.STRIP_HEADER_CONFIG, String.valueOf(stripEnabled));
        config.put(MLAPlugin.CONSUMER_ID_REGISTRY_TOPIC_CONFIG, REGISTRY_TOPIC);
        plugin.configure(config);

        // Start the plugin (this creates the ConsumerIdRegistryClient)
        plugin.start(null);

        // Add the plugin to the broker's plugin list so it's invoked during fetch
        cluster.brokers().values().forEach(broker -> {
            BrokerServer brokerServer = (BrokerServer) broker;
            brokerServer.recordFetchPlugins().add(plugin);
        });

        return plugin;
    }

    /**
     * Wait for the MLAPlugin's registry client to have the User:ANONYMOUS consumer registered.
     */
    private void waitForRegistryReady(MLAPlugin plugin) throws Exception {
        waitForRegistryPrincipal(plugin, ANONYMOUS_PRINCIPAL);
    }

    /**
     * Wait for the MLAPlugin's registry client to have a specific principal registered.
     *
     * @param plugin    the MLAPlugin instance
     * @param principal the principal to wait for
     */
    private void waitForRegistryPrincipal(MLAPlugin plugin, String principal) throws Exception {
        // Use reflection to access the registry client
        java.lang.reflect.Field registryClientField = MLAPlugin.class.getDeclaredField("registryClient");
        registryClientField.setAccessible(true);

        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            ConsumerIdRegistryClient client = (ConsumerIdRegistryClient) registryClientField.get(plugin);
            if (client != null && client.getConsumerId(principal) != null) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new AssertionError(
            "Timed out waiting for MLAPlugin registry client to have " + principal + " registered");
    }

    /**
     * Wait for the MLAPlugin's registry client to no longer have a specific principal registered.
     * Used after deleting a consumer to confirm the tombstone has been processed.
     *
     * @param plugin    the MLAPlugin instance
     * @param principal the principal to wait for removal of
     */
    private void waitForRegistryPrincipalRemoved(MLAPlugin plugin, String principal) throws Exception {
        java.lang.reflect.Field registryClientField = MLAPlugin.class.getDeclaredField("registryClient");
        registryClientField.setAccessible(true);

        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            ConsumerIdRegistryClient client = (ConsumerIdRegistryClient) registryClientField.get(plugin);
            if (client != null && client.getConsumerId(principal) == null) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new AssertionError(
            "Timed out waiting for MLAPlugin registry client to remove " + principal);
    }

    // ---- Helper methods ----

    private KafkaProducer<String, String> createProducer(ClusterInstance cluster) {
        return new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    private KafkaConsumer<String, String> createConsumer(ClusterInstance cluster) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "mla-e2e-group-" + System.nanoTime(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
    }

    /**
     * Poll until the expected number of records is received or the timeout expires.
     */
    private List<ConsumerRecord<String, String>> pollUntilCount(
            KafkaConsumer<String, String> consumer, int expectedCount, long timeoutMs) {
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (allRecords.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : polled) {
                allRecords.add(record);
            }
        }
        return allRecords;
    }
}
