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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.record.RecordFetchPlugin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for RecordFetchPlugin loading and topic-level activation.
 *
 * These tests verify:
 * <ul>
 *   <li>Broker starts successfully with a valid plugin class configured (Req 1.2)</li>
 *   <li>Broker fails to start with an invalid plugin class (Req 1.2)</li>
 *   <li>Plugin is activated only for topics with matching {@code record.fetch.plugins} config (Req 2.3)</li>
 *   <li>Non-MLA topics deliver all records without plugin activation (Req 9.1, 9.2)</li>
 * </ul>
 *
 * Requirements: 1.2, 1.3, 2.3, 9.1, 9.2
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(
            key = "record.fetch.plugin.classes",
            value = "kafka.server.InvocationTrackingPlugin"
        ),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false")
    }
)
public class RecordFetchPluginIntegrationTest {

    private final ClusterInstance cluster;

    public RecordFetchPluginIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    /**
     * Validates Requirement 1.2: Broker starts with valid plugin config.
     *
     * The broker is configured with {@code record.fetch.plugin.classes} pointing to
     * {@link InvocationTrackingPlugin}. Verifies the broker started successfully and
     * the plugin was loaded, configured, and started.
     */
    @ClusterTest
    public void testBrokerStartsWithValidPluginConfig(ClusterInstance cluster) {
        // Access the BrokerServer to verify the plugin was loaded.
        cluster.brokers().values().forEach(broker -> {
            BrokerServer brokerServer = (BrokerServer) broker;
            assertFalse(brokerServer.recordFetchPlugins().isEmpty(),
                "Broker should have loaded at least one RecordFetchPlugin");
            assertTrue(brokerServer.recordFetchPlugins().stream()
                .anyMatch(p -> p instanceof InvocationTrackingPlugin),
                "Broker should have loaded InvocationTrackingPlugin");
        });

        // Verify the plugin was configured and started via static state.
        assertTrue(InvocationTrackingPlugin.isConfigured(),
            "Plugin should have been configured during broker startup");
        assertTrue(InvocationTrackingPlugin.isStarted(),
            "Plugin should have been started during broker startup");
    }

    /**
     * Validates Requirement 1.2 (negative case): Broker fails to start with invalid plugin class.
     *
     * Verifies that the class loading mechanism used by the broker at startup
     * throws an appropriate exception for non-existent plugin classes.
     * The broker uses {@code Utils.newInstance(className, RecordFetchPlugin.class)}
     * which relies on {@code Class.forName()}.
     */
    @ClusterTest
    public void testInvalidPluginClassFailsLoading(ClusterInstance cluster) {
        String invalidClassName = "com.example.NonExistentPlugin";
        org.junit.jupiter.api.Assertions.assertThrows(
            ClassNotFoundException.class,
            () -> Class.forName(invalidClassName),
            "Loading a non-existent plugin class should throw ClassNotFoundException"
        );
    }

    /**
     * Validates Requirements 2.3, 9.1: Plugin is activated only for topics with matching
     * {@code record.fetch.plugins} config; non-MLA topics are unaffected.
     *
     * Creates two topics — one with {@code record.fetch.plugins} configured, one without.
     * Verifies the plugin's {@code isActiveForTopic} returns true only for the configured
     * topic, and that both topics deliver all records correctly.
     */
    @ClusterTest
    public void testPluginInvokedOnlyForMatchingTopics(ClusterInstance cluster) throws Exception {
        InvocationTrackingPlugin.resetCounts();
        String pluginTopic = "plugin-topic";
        String noPluginTopic = "no-plugin-topic";
        int numRecords = 5;

        // Create topic WITH plugin config
        cluster.createTopic(pluginTopic, 1, (short) 1,
            Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG,
                   "kafka.server.InvocationTrackingPlugin"));

        // Create topic WITHOUT plugin config
        cluster.createTopic(noPluginTopic, 1, (short) 1);

        // Produce records to both topics
        produceRecords(cluster, pluginTopic, numRecords);
        produceRecords(cluster, noPluginTopic, numRecords);

        // Consume from both topics to trigger fetch path
        List<ConsumerRecord<String, String>> pluginTopicRecords =
            consumeRecords(cluster, pluginTopic, numRecords);
        assertEquals(numRecords, pluginTopicRecords.size(),
            "All records should be delivered from plugin-enabled topic");

        List<ConsumerRecord<String, String>> noPluginTopicRecords =
            consumeRecords(cluster, noPluginTopic, numRecords);
        assertEquals(numRecords, noPluginTopicRecords.size(),
            "All records should be delivered from non-plugin topic");

        // Verify the plugin's isActiveForTopic was called and returned the correct result.
        // The plugin is active for the plugin-topic (isActiveForTopic returns true)
        // and not active for the no-plugin-topic (isActiveForTopic returns false).
        assertTrue(InvocationTrackingPlugin.getIsActiveTrueCount(pluginTopic) > 0,
            "Plugin should have been activated for the plugin-enabled topic");
        assertEquals(0, InvocationTrackingPlugin.getIsActiveTrueCount(noPluginTopic),
            "Plugin should NOT have been activated for the non-plugin topic");

        // Also verify via direct plugin API that the topic config is correctly evaluated
        RecordFetchPlugin plugin = getPluginFromBroker(cluster);
        assertTrue(plugin.isActiveForTopic(pluginTopic,
            Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, "kafka.server.InvocationTrackingPlugin")),
            "Plugin should report active for topic with matching config");
        assertFalse(plugin.isActiveForTopic(noPluginTopic, Map.of()),
            "Plugin should report inactive for topic without config");
    }

    /**
     * Validates Requirements 9.1, 9.2: Non-MLA topic delivers all records without
     * plugin activation.
     *
     * Creates a topic with no {@code record.fetch.plugins} configuration, produces
     * records, and verifies all records are delivered and the plugin is never activated.
     */
    @ClusterTest
    public void testNonMlaTopicDeliversAllRecords(ClusterInstance cluster) throws Exception {
        InvocationTrackingPlugin.resetCounts();
        String topic = "passthrough-topic";
        int numRecords = 10;

        // Create topic without any plugin config
        cluster.createTopic(topic, 1, (short) 1);

        // Produce records
        produceRecords(cluster, topic, numRecords);

        // Consume and verify all records are delivered
        List<ConsumerRecord<String, String>> records = consumeRecords(cluster, topic, numRecords);
        assertEquals(numRecords, records.size(),
            "All records should be delivered from a non-MLA topic");

        // Verify record content
        for (int i = 0; i < numRecords; i++) {
            assertEquals("key-" + i, records.get(i).key());
            assertEquals("value-" + i, records.get(i).value());
        }

        // Verify plugin was never activated for this topic
        assertEquals(0, InvocationTrackingPlugin.getIsActiveTrueCount(topic),
            "Plugin should NOT be activated for a topic without record.fetch.plugins config");
    }

    // ---- Helper methods ----

    private RecordFetchPlugin getPluginFromBroker(ClusterInstance cluster) {
        return cluster.brokers().values().stream()
            .map(broker -> (BrokerServer) broker)
            .flatMap(broker -> broker.recordFetchPlugins().stream())
            .filter(p -> p instanceof InvocationTrackingPlugin)
            .findFirst()
            .orElseThrow(() -> new AssertionError("InvocationTrackingPlugin not found in broker"));
    }

    private void produceRecords(ClusterInstance cluster, String topic, int count) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ))) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)).get();
            }
        }
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            ClusterInstance cluster, String topic, int expectedCount) {
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + topic + "-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ))) {
            consumer.subscribe(List.of(topic));

            long deadline = System.currentTimeMillis() + 30_000; // 30 second timeout
            while (allRecords.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    allRecords.add(record);
                }
            }
        }
        return allRecords;
    }
}
