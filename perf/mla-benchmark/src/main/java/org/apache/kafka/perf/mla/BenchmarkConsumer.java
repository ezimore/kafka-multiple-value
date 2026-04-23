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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Benchmark consumer that supports all three routing options.
 *
 * Usage:
 *   java BenchmarkConsumer <option> <consumerId> <bootstrap> <topics>
 *                          <filterEventIds> <timeoutSec> <reportIntervalMs>
 *
 * Options:
 *   1 = client-side filtering (subscribe to topic, filter by event-id header)
 *   2 = MLA broker-side filtering (subscribe to topic, broker filters for us)
 *   3 = topic-per-audience (subscribe to assigned topic, no filtering needed)
 *
 * consumerId: 1, 2, or 3 (identifies which consumer this is)
 * topics: comma-separated topic list to subscribe to
 * filterEventIds: comma-separated event IDs to accept (option 1 only, ignored for 2/3)
 * timeoutSec: stop after this many seconds of no new records
 */
public class BenchmarkConsumer {

    private static final String EVENT_ID_HEADER = "event-id";

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: BenchmarkConsumer <option> <consumerId> <bootstrap> "
                + "<topics> <filterEventIds> <timeoutSec> <reportIntervalMs>");
            System.exit(1);
        }

        int option = Integer.parseInt(args[0]);
        int consumerId = Integer.parseInt(args[1]);
        String bootstrap = args[2];
        String topicsStr = args[3];
        String filterStr = args[4];
        int timeoutSec = Integer.parseInt(args[5]);
        long reportIntervalMs = Long.parseLong(args[6]);

        // Parse filter event IDs (for option 1)
        Set<String> filterEventIds = new HashSet<>();
        if (!filterStr.equals("none")) {
            for (String id : filterStr.split(",")) {
                filterEventIds.add(id.trim());
            }
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-consumer-" + consumerId + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 64);  // 64KB min fetch
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        String[] topics = topicsStr.split(",");

        System.out.printf("=== CONSUMER %d Option %d ===%n", consumerId, option);
        System.out.printf("Topics: %s, Filter: %s%n", topicsStr,
            filterEventIds.isEmpty() ? "none (broker-filtered or topic-routed)" : filterEventIds);

        long totalRecords = 0;
        long totalBytes = 0;
        long totalAccepted = 0;
        long totalFiltered = 0;

        long startMs = System.currentTimeMillis();
        long lastRecordMs = startMs;
        long windowStart = startMs;
        long windowAccepted = 0;
        long windowBytes = 0;
        long timeoutMs = timeoutSec * 1000L;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topics));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();

                if (!records.isEmpty()) {
                    lastRecordMs = now;
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    totalRecords++;
                    int recordSize = (record.value() != null ? record.value().length : 0)
                        + (record.key() != null ? record.key().getBytes(StandardCharsets.UTF_8).length : 0);

                    if (option == 1) {
                        // Client-side filtering: check event-id header
                        String eventId = getHeaderValue(record, EVENT_ID_HEADER);
                        if (eventId != null && filterEventIds.contains(eventId)) {
                            totalAccepted++;
                            totalBytes += recordSize;
                            windowAccepted++;
                            windowBytes += recordSize;
                        } else {
                            totalFiltered++;
                        }
                    } else {
                        // Options 2 and 3: all received records are accepted
                        totalAccepted++;
                        totalBytes += recordSize;
                        windowAccepted++;
                        windowBytes += recordSize;
                    }
                }

                // Progress reporting
                if (now - windowStart >= reportIntervalMs) {
                    double elapsed = (now - windowStart) / 1000.0;
                    double recsPerSec = windowAccepted / elapsed;
                    double mbPerSec = windowBytes / elapsed / (1024.0 * 1024.0);
                    System.out.printf("[CONSUMER-%d] %d accepted (%.0f/sec, %.2f MB/sec), %d total, %d filtered%n",
                        consumerId, totalAccepted, recsPerSec, mbPerSec, totalRecords, totalFiltered);
                    windowStart = now;
                    windowAccepted = 0;
                    windowBytes = 0;
                }

                // Timeout check
                if (now - lastRecordMs > timeoutMs) {
                    System.out.printf("[CONSUMER-%d] No records for %d sec, stopping.%n",
                        consumerId, timeoutSec);
                    break;
                }
            }
        }

        long endMs = System.currentTimeMillis();
        double elapsedSec = (endMs - startMs) / 1000.0;
        double recsPerSec = totalAccepted / elapsedSec;
        double mbPerSec = totalBytes / elapsedSec / (1024.0 * 1024.0);

        System.out.println();
        System.out.println("=== CONSUMER RESULTS ===");
        System.out.printf("option=%d%n", option);
        System.out.printf("consumer_id=%d%n", consumerId);
        System.out.printf("total_records_received=%d%n", totalRecords);
        System.out.printf("total_records_accepted=%d%n", totalAccepted);
        System.out.printf("total_records_filtered=%d%n", totalFiltered);
        System.out.printf("total_bytes=%d%n", totalBytes);
        System.out.printf("elapsed_sec=%.2f%n", elapsedSec);
        System.out.printf("accepted_records_per_sec=%.0f%n", recsPerSec);
        System.out.printf("mb_per_sec=%.2f%n", mbPerSec);
    }

    private static String getHeaderValue(ConsumerRecord<String, byte[]> record, String headerKey) {
        for (Header header : record.headers()) {
            if (headerKey.equals(header.key())) {
                return new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
