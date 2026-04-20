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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Test-only implementation of the Data Management Administration role.
 * <p>
 * Manages the Consumer ID Registry topic for integration and property-based
 * testing. Wraps a Kafka {@link Admin} and {@link KafkaProducer} to create
 * the registry topic, register consumers with sequential IDs, and delete
 * consumers by writing tombstones.
 * <p>
 * Consumer IDs are auto-incremented integers starting from 0. Once an ID is
 * assigned, it is never reused — even after the consumer is deleted, the
 * counter continues incrementing.
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6</b></p>
 */
public class TestAdminClient implements Closeable {

    private final Admin adminClient;
    private final KafkaProducer<String, byte[]> producer;

    /**
     * The next consumer ID to assign. Monotonically increasing, never reset.
     */
    private int nextConsumerId;

    /**
     * The registry topic name, set when {@link #createRegistryTopic(String, int)} is called.
     */
    private String registryTopicName;

    /**
     * Tracks which principal was assigned which consumer ID, for tombstone writes.
     */
    private final Map<String, Integer> principalToId;

    /**
     * The set of consumer IDs that have been retired (deleted). These IDs
     * must never be reassigned.
     */
    private final Set<Integer> retiredIds;

    /**
     * Create a new TestAdminClient connected to the given Kafka cluster.
     *
     * @param bootstrapServers the Kafka bootstrap servers connection string
     */
    public TestAdminClient(String bootstrapServers) {
        // Set up AdminClient
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = Admin.create(adminProps);

        // Set up KafkaProducer for writing to the registry topic
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(producerProps);

        this.nextConsumerId = 0;
        this.principalToId = new HashMap<>();
        this.retiredIds = new HashSet<>();
    }

    /**
     * Create the Consumer ID Registry topic as a compacted topic.
     *
     * @param topicName  the name of the registry topic
     * @param partitions the number of partitions
     * @throws ExecutionException   if topic creation fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void createRegistryTopic(String topicName, int partitions) throws ExecutionException, InterruptedException {
        this.registryTopicName = topicName;
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        Map<String, String> configs = new HashMap<>();
        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        newTopic.configs(configs);
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    }

    /**
     * Register a consumer principal and assign the next auto-incremented Consumer ID.
     * <p>
     * Writes a record to the registry topic with the principal as the key and
     * the 4-byte big-endian consumer ID as the value.
     *
     * @param principal the Kafka user principal (e.g., {@code "User:alice"})
     * @return the assigned consumer ID
     * @throws ExecutionException   if the write fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public int registerConsumer(String principal) throws ExecutionException, InterruptedException {
        int consumerId = nextConsumerId++;
        principalToId.put(principal, consumerId);

        // Encode consumer ID as 4-byte big-endian integer
        byte[] value = ByteBuffer.allocate(4).putInt(consumerId).array();

        // Write to the first partition (key-based partitioning will be used in production,
        // but for test purposes we let the default partitioner handle it)
        producer.send(new ProducerRecord<>(getRegistryTopicFromLastCreate(), principal, value)).get();

        return consumerId;
    }

    /**
     * Delete a consumer by writing a tombstone (null value) to the registry topic.
     * <p>
     * The consumer's ID is tracked as retired and will never be reassigned.
     *
     * @param principal the Kafka user principal to delete
     * @throws ExecutionException   if the write fails
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws IllegalArgumentException if the principal was never registered
     */
    public void deleteConsumer(String principal) throws ExecutionException, InterruptedException {
        Integer consumerId = principalToId.get(principal);
        if (consumerId == null) {
            throw new IllegalArgumentException("Principal '" + principal + "' is not registered");
        }

        retiredIds.add(consumerId);
        principalToId.remove(principal);

        // Write tombstone (null value) to the registry topic
        producer.send(new ProducerRecord<>(getRegistryTopicFromLastCreate(), principal, null)).get();
    }

    /**
     * Get the next Consumer ID that would be assigned.
     * <p>
     * Useful for test assertions to verify sequential assignment.
     *
     * @return the next consumer ID
     */
    public int getNextConsumerId() {
        return nextConsumerId;
    }

    /**
     * Get the set of retired (deleted) Consumer IDs.
     * <p>
     * Useful for test assertions to verify the never-reuse invariant.
     *
     * @return an unmodifiable view of the retired consumer IDs
     */
    public Set<Integer> getRetiredIds() {
        return Collections.unmodifiableSet(new HashSet<>(retiredIds));
    }

    @Override
    public void close() {
        producer.close();
        adminClient.close();
    }

    private String getRegistryTopicFromLastCreate() {
        if (registryTopicName == null) {
            throw new IllegalStateException("Registry topic has not been created yet. Call createRegistryTopic() first.");
        }
        return registryTopicName;
    }
}
