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
package org.apache.kafka.perf.mla;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Sets up the Consumer ID Registry topic and registers consumers for MLA benchmarks.
 *
 * Usage:
 *   java SetupRegistry <bootstrap> <registryTopic> [consumer1Principal consumer2Principal consumer3Principal]
 *
 * Default principals (SASL mode):
 *   User:alice    -> ID 0 (consumer1)
 *   User:bob      -> ID 1 (consumer2)
 *   User:carol    -> ID 2 (consumer3)
 */
public class SetupRegistry {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SetupRegistry <bootstrap> <registryTopic> [principal1 principal2 principal3]");
            System.exit(1);
        }

        String bootstrap = args[0];
        String registryTopic = args[1];

        // Consumer principals — use args if provided, otherwise defaults
        String principal1 = args.length > 2 ? args[2] : "User:alice";
        String principal2 = args.length > 3 ? args[3] : "User:bob";
        String principal3 = args.length > 4 ? args[4] : "User:carol";

        // Create the registry topic
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (Admin admin = Admin.create(adminProps)) {
            NewTopic topic = new NewTopic(registryTopic, 1, (short) 1);
            Map<String, String> configs = new HashMap<>();
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            topic.configs(configs);

            try {
                admin.createTopics(Collections.singleton(topic)).all().get();
                System.out.println("Created registry topic: " + registryTopic);
            } catch (Exception e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    System.out.println("Registry topic already exists: " + registryTopic);
                } else {
                    throw e;
                }
            }
        }

        // Register consumers
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            String[][] consumers = {
                {principal1, "0"},  // consumer1
                {principal2, "1"},  // consumer2
                {principal3, "2"},  // consumer3
            };

            for (String[] entry : consumers) {
                String principal = entry[0];
                int consumerId = Integer.parseInt(entry[1]);
                byte[] value = ByteBuffer.allocate(4).putInt(consumerId).array();
                producer.send(new ProducerRecord<>(registryTopic, principal, value)).get();
                System.out.printf("Registered %s -> consumer ID %d%n", principal, consumerId);
            }
        }

        System.out.println("Registry setup complete.");
    }
}
