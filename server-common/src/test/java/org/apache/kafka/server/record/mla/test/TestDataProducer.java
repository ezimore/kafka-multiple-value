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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.security.mla.AuthorizationBitmap;
import org.apache.kafka.common.security.mla.ConsumerIdRegistryClient;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Test-only Data Producer that reads consumer IDs from the registry,
 * constructs authorization bitmaps, and produces records to Kafka.
 * <p>
 * Internally wraps a {@link KafkaProducer} and a {@link ConsumerIdRegistryClient}.
 * For each record, the producer resolves the authorized principals to their
 * Consumer IDs, builds an authorization bitmap using
 * {@link AuthorizationBitmap#create(Set)}, and attaches it as the
 * {@code mla-authz-bitmap} record header before sending.
 *
 * <p><b>Validates: Requirements 4.1, 5.1, 5.6</b></p>
 */
public class TestDataProducer implements Closeable {

    /** The header key used for the authorization bitmap. */
    public static final String MLA_AUTHZ_HEADER_KEY = "mla-authz-bitmap";

    private final KafkaProducer<String, String> producer;
    private final ConsumerIdRegistryClient registryClient;

    /**
     * Create a new TestDataProducer connected to the given Kafka cluster.
     *
     * @param bootstrapServers the Kafka bootstrap servers connection string
     * @param registryTopic    the Consumer ID Registry topic name
     */
    public TestDataProducer(String bootstrapServers, String registryTopic) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(producerProps);

        this.registryClient = new ConsumerIdRegistryClient(bootstrapServers, registryTopic);
        this.registryClient.start();
    }

    /**
     * Produce a record with an authorization bitmap header.
     * <p>
     * Steps:
     * <ol>
     *   <li>Resolve each principal in {@code authorizedPrincipals} to a Consumer ID
     *       via the {@link ConsumerIdRegistryClient}.</li>
     *   <li>Build the authorization bitmap using {@link AuthorizationBitmap#create(Set)}
     *       with the resolved Consumer IDs.</li>
     *   <li>Construct a {@link ProducerRecord} with the key, value, and an
     *       {@code mla-authz-bitmap} header containing the bitmap bytes.</li>
     *   <li>Send the record synchronously and return the {@link RecordMetadata}.</li>
     * </ol>
     *
     * @param topic                the target topic
     * @param key                  the record key
     * @param value                the record value
     * @param authorizedPrincipals the set of principals authorized to receive this record
     * @return the metadata for the sent record
     * @throws IllegalArgumentException if any principal is not found in the registry
     * @throws ExecutionException       if the send fails
     * @throws InterruptedException     if the thread is interrupted while waiting
     */
    public RecordMetadata produce(String topic, String key, String value,
                                  Set<String> authorizedPrincipals) throws ExecutionException, InterruptedException {
        // Step 1: Resolve principals to Consumer IDs
        Set<Integer> consumerIds = new HashSet<>();
        for (String principal : authorizedPrincipals) {
            Integer consumerId = registryClient.getConsumerId(principal);
            if (consumerId == null) {
                throw new IllegalArgumentException(
                        "Principal '" + principal + "' is not found in the Consumer ID Registry");
            }
            consumerIds.add(consumerId);
        }

        // Step 2: Build the authorization bitmap
        byte[] bitmap = AuthorizationBitmap.create(consumerIds);

        // Step 3: Construct the ProducerRecord with the authorization header
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        record.headers().add(new RecordHeader(MLA_AUTHZ_HEADER_KEY, bitmap));

        // Step 4: Send synchronously and return metadata
        return producer.send(record).get();
    }

    /**
     * Refresh the local consumer ID cache from the registry topic.
     * <p>
     * The {@link ConsumerIdRegistryClient} runs a background polling thread,
     * so this method simply waits briefly to allow the background thread to
     * pick up any recent changes. In practice, the cache is eventually
     * consistent with the registry topic.
     */
    public void refreshConsumerIds() {
        // The registry client's background thread continuously polls for updates.
        // Allow a short delay for the background thread to process recent records.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Close the internal {@link KafkaProducer} and {@link ConsumerIdRegistryClient}.
     */
    @Override
    public void close() {
        producer.close();
        registryClient.close();
    }
}
