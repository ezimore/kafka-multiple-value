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
package org.apache.kafka.common.security.mla;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Reads and caches consumer ID mappings from the Consumer ID Registry topic.
 * <p>
 * Used by both the MLAPlugin (broker-side) and Data Producers (client-side)
 * to resolve Kafka user principals to their auto-incremented Consumer IDs.
 * <p>
 * The client starts a background thread that continuously polls the registry
 * topic and maintains a thread-safe in-memory cache of principal-to-ID
 * mappings. Tombstone records (null value) cause the corresponding principal
 * to be removed from the cache.
 */
public class ConsumerIdRegistryClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerIdRegistryClient.class);

    /** Consumer ID values are 4-byte big-endian integers. */
    private static final int CONSUMER_ID_BYTE_LENGTH = 4;

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final String bootstrapServers;
    private final String registryTopic;
    private final ConcurrentHashMap<String, Integer> cache;
    private final AtomicBoolean running;

    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile Thread pollingThread;

    /**
     * @param bootstrapServers Kafka bootstrap servers
     * @param registryTopic    the Consumer ID Registry topic name
     */
    public ConsumerIdRegistryClient(String bootstrapServers, String registryTopic) {
        this.bootstrapServers = bootstrapServers;
        this.registryTopic = registryTopic;
        this.cache = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
    }

    /**
     * Start consuming the registry topic in a background thread.
     * <p>
     * Uses manual partition assignment ({@code assign()}) rather than
     * {@code subscribe()} to avoid group coordination overhead. Seeks to
     * the beginning of all partitions on start.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("ConsumerIdRegistryClient is already running");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);

        // Manually assign all partitions and seek to beginning
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(registryTopic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            log.warn("No partitions found for registry topic '{}'. Cache will remain empty until partitions are available.", registryTopic);
            consumer.close();
            consumer = null;
            running.set(false);
            return;
        }

        List<TopicPartition> partitions = partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toList());

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        pollingThread = new Thread(this::pollLoop, "consumer-id-registry-client");
        pollingThread.setDaemon(true);
        pollingThread.start();

        log.info("ConsumerIdRegistryClient started for topic '{}' with {} partition(s)",
                registryTopic, partitions.size());
    }

    /**
     * Get the consumer ID for a principal, or null if not registered.
     *
     * @param principal the Kafka user principal (e.g., {@code "User:alice"})
     * @return the consumer ID, or null if the principal is not in the cache
     */
    public Integer getConsumerId(String principal) {
        return cache.get(principal);
    }

    /**
     * Get all current principal-to-ID mappings.
     *
     * @return an unmodifiable snapshot of the current mappings
     */
    public Map<String, Integer> getAllMappings() {
        return Collections.unmodifiableMap(new HashMap<>(cache));
    }

    /**
     * Close the background consumer and release resources.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info("Shutting down ConsumerIdRegistryClient");

        // Interrupt the polling thread and wait for it to finish
        if (pollingThread != null) {
            pollingThread.interrupt();
            try {
                pollingThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for polling thread to stop");
            }
            pollingThread = null;
        }

        if (consumer != null) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } catch (Exception e) {
                log.warn("Error closing consumer", e);
            }
            consumer = null;
        }
    }

    /**
     * Background polling loop that continuously reads from the registry topic
     * and updates the in-memory cache.
     */
    private void pollLoop() {
        try {
            while (running.get()) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        processRecord(record);
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        log.error("Error polling consumer ID registry topic", e);
                    }
                }
            }
        } finally {
            log.debug("Polling loop exited for registry topic '{}'", registryTopic);
        }
    }

    /**
     * Process a single record from the registry topic.
     * <p>
     * If the value is null (tombstone), the principal is removed from the cache.
     * Otherwise, the value is decoded as a 4-byte big-endian integer and stored
     * as the consumer ID for the principal.
     */
    private void processRecord(ConsumerRecord<String, byte[]> record) {
        String principal = record.key();
        if (principal == null) {
            log.warn("Ignoring registry record with null key at offset {} partition {}",
                    record.offset(), record.partition());
            return;
        }

        byte[] value = record.value();
        if (value == null) {
            // Tombstone — remove the principal from the cache
            Integer removed = cache.remove(principal);
            if (removed != null) {
                log.debug("Removed principal '{}' (consumer ID {}) from cache (tombstone)",
                        principal, removed);
            }
        } else if (value.length != CONSUMER_ID_BYTE_LENGTH) {
            log.warn("Ignoring registry record for principal '{}' with unexpected value length {} (expected {})",
                    principal, value.length, CONSUMER_ID_BYTE_LENGTH);
        } else {
            int consumerId = ByteBuffer.wrap(value).getInt();
            cache.put(principal, consumerId);
            log.debug("Updated cache: principal '{}' → consumer ID {}", principal, consumerId);
        }
    }
}
