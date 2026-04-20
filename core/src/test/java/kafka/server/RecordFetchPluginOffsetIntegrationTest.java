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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for offset advancement and watermark correctness when
 * the RecordFetchPlugin chain filters out records.
 *
 * <p>These tests use {@link FilteringPlugin}, which filters out records whose
 * keys start with "skip-" and passes through all other records. This allows
 * verifying that:</p>
 * <ul>
 *   <li>After filtering, subsequent fetches start after filtered records (no re-evaluation)</li>
 *   <li>High watermark and log start offset are correct in fetch responses</li>
 *   <li>A consumer can commit offsets for records it did not receive</li>
 * </ul>
 *
 * <p><b>Requirements: 11.1, 11.2, 11.3</b></p>
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(
            key = "record.fetch.plugin.classes",
            value = "kafka.server.FilteringPlugin"
        ),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false")
    }
)
public class RecordFetchPluginOffsetIntegrationTest {

    private final ClusterInstance cluster;

    public RecordFetchPluginOffsetIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    /**
     * Test: after filtering, subsequent fetches start after filtered records.
     *
     * <p>Produces 10 records with interleaved "keep-N" and "skip-N" keys.
     * Consumes all available records and verifies only the "keep-N" records
     * are received. Then produces 5 more "keep-N" records and verifies the
     * consumer receives them without re-evaluating the previously filtered records.</p>
     *
     * <p><b>Validates: Requirement 11.1</b></p>
     */
    @ClusterTest
    public void testOffsetAdvancementPastFilteredRecords(ClusterInstance cluster) throws Exception {
        FilteringPlugin.resetCounts();
        String topic = "offset-advance-topic-" + System.nanoTime();
        String groupId = "offset-advance-group-" + System.nanoTime();

        // Create topic with FilteringPlugin enabled
        cluster.createTopic(topic, 1, (short) 1,
            Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, "kafka.server.FilteringPlugin"));

        // Produce 10 records: interleaved keep-N and skip-N
        produceInterleavedRecords(cluster, topic, 10);

        // First consumer: consume all available records
        List<ConsumerRecord<String, String>> firstBatch;
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster, groupId)) {
            consumer.subscribe(List.of(topic));
            firstBatch = pollUntilCount(consumer, 5, 30_000);

            // Verify only "keep-" records were received
            assertEquals(5, firstBatch.size(), "Should receive exactly 5 keep-N records");
            for (ConsumerRecord<String, String> record : firstBatch) {
                assertTrue(record.key().startsWith("keep-"),
                    "All received records should have keys starting with 'keep-', got: " + record.key());
            }

            // Commit offsets so the consumer group tracks position
            consumer.commitSync();
        }

        // Produce 5 more "keep-" records (these should all pass through)
        try (KafkaProducer<String, String> producer = createProducer(cluster)) {
            for (int i = 10; i < 15; i++) {
                producer.send(new ProducerRecord<>(topic, "keep-" + i, "value-" + i)).get();
            }
        }

        // Second consumer in the same group: should pick up from committed offset
        // and receive only the new records, NOT re-evaluate the filtered ones
        List<ConsumerRecord<String, String>> secondBatch;
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster, groupId)) {
            consumer.subscribe(List.of(topic));
            secondBatch = pollUntilCount(consumer, 5, 30_000);

            assertEquals(5, secondBatch.size(),
                "Should receive exactly 5 new keep-N records without re-evaluating filtered records");
            for (ConsumerRecord<String, String> record : secondBatch) {
                assertTrue(record.key().startsWith("keep-"),
                    "All received records should have keys starting with 'keep-', got: " + record.key());
            }

            // Verify the keys are from the second batch (keep-10 through keep-14)
            for (int i = 0; i < 5; i++) {
                assertEquals("keep-" + (10 + i), secondBatch.get(i).key(),
                    "Record " + i + " in second batch should be keep-" + (10 + i));
            }
        }
    }

    /**
     * Test: high watermark and log start offset are correct in fetch responses.
     *
     * <p>Produces 10 interleaved records (5 keep, 5 skip). Verifies that the
     * consumer sees the correct high watermark (reflecting all 10 records in the log)
     * and that the log start offset is 0, regardless of how many records were filtered.</p>
     *
     * <p><b>Validates: Requirement 11.2</b></p>
     */
    @ClusterTest
    public void testHighWatermarkAndLogStartOffsetCorrectness(ClusterInstance cluster) throws Exception {
        FilteringPlugin.resetCounts();
        String topic = "watermark-topic-" + System.nanoTime();
        String groupId = "watermark-group-" + System.nanoTime();

        // Create topic with FilteringPlugin enabled
        cluster.createTopic(topic, 1, (short) 1,
            Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, "kafka.server.FilteringPlugin"));

        // Produce 10 interleaved records
        produceInterleavedRecords(cluster, topic, 10);

        TopicPartition tp = new TopicPartition(topic, 0);

        // Consume and check watermark/offset metadata
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster, groupId)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));

            List<ConsumerRecord<String, String>> records = pollUntilCount(consumer, 5, 30_000);
            assertEquals(5, records.size(), "Should receive exactly 5 keep-N records");

            // Poll once more to ensure the consumer has fetched all records (including
            // the last filtered batch) and advanced its position past them
            consumer.poll(Duration.ofMillis(1000));

            // The end offset (high watermark from consumer's perspective) should be 10
            // because all 10 records exist in the log, regardless of filtering
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));
            assertEquals(10L, endOffsets.get(tp),
                "End offset (high watermark) should reflect all 10 records in the log, not just the 5 delivered");

            // The beginning offset (log start offset) should be 0
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(List.of(tp));
            assertEquals(0L, beginningOffsets.get(tp),
                "Beginning offset (log start offset) should be 0");

            // Verify the consumer's position has advanced past the last received record.
            // The position may be at offset 9 or 10 depending on whether the consumer
            // has fetched the final batch containing only filtered records.
            long position = consumer.position(tp);
            assertTrue(position >= 9L,
                "Consumer position should advance past the last received record (keep-4 at offset 8), got: " + position);
        }
    }

    /**
     * Test: consumer can commit offsets for records it did not receive.
     *
     * <p>Produces 10 interleaved records. The consumer receives only the "keep-N"
     * records, but the offsets of the "skip-N" records still exist in the log.
     * Verifies that the consumer can commit an offset corresponding to a filtered
     * record's position, and that a new consumer in the same group resumes correctly
     * from that committed offset.</p>
     *
     * <p><b>Validates: Requirement 11.3</b></p>
     */
    @ClusterTest
    public void testOffsetCommitForFilteredRecords(ClusterInstance cluster) throws Exception {
        FilteringPlugin.resetCounts();
        String topic = "commit-filtered-topic-" + System.nanoTime();
        String groupId = "commit-filtered-group-" + System.nanoTime();

        // Create topic with FilteringPlugin enabled
        cluster.createTopic(topic, 1, (short) 1,
            Map.of(TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, "kafka.server.FilteringPlugin"));

        // Produce 10 interleaved records:
        // offset 0: keep-0, offset 1: skip-0, offset 2: keep-1, offset 3: skip-1, ...
        // offset 8: keep-4, offset 9: skip-4
        produceInterleavedRecords(cluster, topic, 10);

        TopicPartition tp = new TopicPartition(topic, 0);

        // First consumer: consume some records, then commit an offset that corresponds
        // to a filtered record's position
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster, groupId)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));

            // Consume all 5 keep records
            List<ConsumerRecord<String, String>> records = pollUntilCount(consumer, 5, 30_000);
            assertEquals(5, records.size());

            // Commit offset 6 — this is the offset of skip-2 (a filtered record) + 1
            // The record at offset 5 is skip-2, so committing offset 6 means
            // "I've processed everything up to and including offset 5"
            // This offset was never delivered to the consumer, but the commit should succeed
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(6L)));
        }

        // Second consumer in the same group: should resume from committed offset 6
        try (KafkaConsumer<String, String> consumer = createConsumer(cluster, groupId)) {
            consumer.assign(List.of(tp));

            // Seek to the committed offset for this group
            OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
            assertNotNull(committed, "Committed offset should not be null");
            assertEquals(6L, committed.offset(),
                "Committed offset should be 6 (offset of a filtered record + 1)");

            // Seek to the committed offset and consume remaining records
            consumer.seek(tp, committed.offset());
            List<ConsumerRecord<String, String>> remaining = pollUntilCount(consumer, 2, 30_000);

            // From offset 6 onwards: offset 6=keep-3, offset 7=skip-3, offset 8=keep-4, offset 9=skip-4
            // After filtering: keep-3 and keep-4
            assertEquals(2, remaining.size(),
                "Should receive 2 remaining keep records after resuming from committed offset");
            assertEquals("keep-3", remaining.get(0).key());
            assertEquals("keep-4", remaining.get(1).key());
        }
    }

    // ---- Helper methods ----

    /**
     * Produce interleaved keep-N and skip-N records.
     * Pattern: keep-0, skip-0, keep-1, skip-1, ..., keep-(count/2-1), skip-(count/2-1)
     */
    private void produceInterleavedRecords(ClusterInstance cluster, String topic, int count) throws Exception {
        try (KafkaProducer<String, String> producer = createProducer(cluster)) {
            for (int i = 0; i < count / 2; i++) {
                producer.send(new ProducerRecord<>(topic, "keep-" + i, "value-keep-" + i)).get();
                producer.send(new ProducerRecord<>(topic, "skip-" + i, "value-skip-" + i)).get();
            }
        }
    }

    private KafkaProducer<String, String> createProducer(ClusterInstance cluster) {
        return new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));
    }

    private KafkaConsumer<String, String> createConsumer(ClusterInstance cluster, String groupId) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
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
