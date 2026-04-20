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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Test-only Data Consumer that subscribes to a topic and collects
 * received records for assertion in tests.
 * <p>
 * Wraps a {@link KafkaConsumer} with String key and value deserializers.
 * Each instance uses a unique UUID-based group ID to avoid conflicts
 * between test consumers.
 *
 * <p><b>Validates: Requirements 7.1, 8.1, 8.2</b></p>
 */
public class TestDataConsumer implements Closeable {

    private final KafkaConsumer<String, String> consumer;
    private final String principal;

    /**
     * Create a new TestDataConsumer connected to the given Kafka cluster.
     *
     * @param bootstrapServers the Kafka bootstrap servers connection string
     * @param principal        the principal name this consumer is authenticated as
     *                         (e.g., {@code "User:alice"})
     */
    public TestDataConsumer(String bootstrapServers, String principal) {
        this.principal = principal;

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    /**
     * Subscribe to the given topic.
     *
     * @param topic the topic name to subscribe to
     */
    public void subscribe(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Poll for records and return them as a list.
     *
     * @param timeout the maximum time to block waiting for records
     * @return a list of consumer records received during the poll
     */
    public List<ConsumerRecord<String, String>> poll(Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record);
        }
        return result;
    }

    /**
     * Get the principal this consumer is authenticated as.
     *
     * @return the principal name (e.g., {@code "User:alice"})
     */
    public String getPrincipal() {
        return principal;
    }

    /**
     * Close the internal {@link KafkaConsumer}.
     */
    @Override
    public void close() {
        consumer.close();
    }
}
