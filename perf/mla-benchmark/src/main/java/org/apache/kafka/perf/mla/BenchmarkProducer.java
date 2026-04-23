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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.security.mla.AuthorizationBitmap;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark producer that supports all three routing options.
 *
 * Usage:
 *   java BenchmarkProducer <option> <bootstrap> <numEvents> <minSize> <maxSize>
 *                          <numEventIds> <batchSize> <lingerMs> <reportIntervalMs>
 *
 * Options:
 *   1 = client-side filtering (single topic, event-id header)
 *   2 = MLA broker-side filtering (single topic, event-id header + mla-authz-bitmap)
 *   3 = topic-per-audience (route to event1/event2/event3 topics)
 */
public class BenchmarkProducer {

    // MLA authorization mapping: event-id -> set of authorized consumer IDs
    // event-id 1 -> consumer1 (ID 0), consumer2 (ID 1)
    // event-id 2 -> consumer2 (ID 1)
    // event-id 3 -> consumer2 (ID 1), consumer3 (ID 2)
    // event-id 4 -> consumer3 (ID 2)
    private static final Map<Integer, Set<Integer>> EVENT_ID_TO_CONSUMER_IDS = new HashMap<>();
    static {
        EVENT_ID_TO_CONSUMER_IDS.put(1, new HashSet<>(Arrays.asList(0, 1)));
        EVENT_ID_TO_CONSUMER_IDS.put(2, new HashSet<>(Arrays.asList(1)));
        EVENT_ID_TO_CONSUMER_IDS.put(3, new HashSet<>(Arrays.asList(1, 2)));
        EVENT_ID_TO_CONSUMER_IDS.put(4, new HashSet<>(Arrays.asList(2)));
    }

    // Topic routing for option 3: event-id -> list of target topics
    // event-id 1 -> event1, event2
    // event-id 2 -> event2
    // event-id 3 -> event2, event3
    // event-id 4 -> event3
    private static final Map<Integer, String[]> EVENT_ID_TO_TOPICS = new HashMap<>();
    static {
        EVENT_ID_TO_TOPICS.put(1, new String[]{"event1", "event2"});
        EVENT_ID_TO_TOPICS.put(2, new String[]{"event2"});
        EVENT_ID_TO_TOPICS.put(3, new String[]{"event2", "event3"});
        EVENT_ID_TO_TOPICS.put(4, new String[]{"event3"});
    }

    private static final String MLA_AUTHZ_HEADER = "mla-authz-bitmap";
    private static final String EVENT_ID_HEADER = "event-id";

    public static void main(String[] args) throws Exception {
        if (args.length < 9) {
            System.err.println("Usage: BenchmarkProducer <option> <bootstrap> <numEvents> "
                + "<minSize> <maxSize> <numEventIds> <batchSize> <lingerMs> <reportIntervalMs>");
            System.exit(1);
        }

        int option = Integer.parseInt(args[0]);
        String bootstrap = args[1];
        long numEvents = Long.parseLong(args[2]);
        int minSize = Integer.parseInt(args[3]);
        int maxSize = Integer.parseInt(args[4]);
        int numEventIds = Integer.parseInt(args[5]);
        int batchSize = Integer.parseInt(args[6]);
        int lingerMs = Integer.parseInt(args[7]);
        long reportIntervalMs = Long.parseLong(args[8]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128 * 1024 * 1024); // 128MB

        // Pre-compute MLA bitmaps for option 2
        Map<Integer, byte[]> bitmapCache = new HashMap<>();
        if (option == 2) {
            for (Map.Entry<Integer, Set<Integer>> entry : EVENT_ID_TO_CONSUMER_IDS.entrySet()) {
                bitmapCache.put(entry.getKey(), AuthorizationBitmap.create(entry.getValue()));
            }
        }

        SplittableRandom random = new SplittableRandom(42);
        AtomicLong totalSent = new AtomicLong(0);
        AtomicLong totalBytes = new AtomicLong(0);
        AtomicLong totalLatency = new AtomicLong(0);
        AtomicLong maxLatency = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        // Latency sampling (sample 1 in 100 for percentile calculation)
        int sampleSize = (int) Math.min(numEvents / 100 + 1, 500000);
        int[] latencySamples = new int[sampleSize];
        AtomicLong sampleIndex = new AtomicLong(0);

        String topicName = (option == 3) ? null : "events";

        System.out.printf("=== PRODUCER Option %d ===%n", option);
        System.out.printf("Events: %d, Size: %d-%d bytes, EventIDs: 1-%d%n",
            numEvents, minSize, maxSize, numEventIds);

        long startMs = System.currentTimeMillis();
        long windowStart = startMs;
        long windowCount = 0;
        long windowBytes = 0;

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            for (long i = 0; i < numEvents; i++) {
                int eventId = random.nextInt(numEventIds) + 1; // 1..numEventIds
                int payloadSize = minSize + random.nextInt(maxSize - minSize + 1);
                byte[] payload = new byte[payloadSize];
                random.nextBytes(payload);

                byte[] eventIdBytes = String.valueOf(eventId).getBytes(StandardCharsets.UTF_8);
                long sendStartMs = System.currentTimeMillis();

                Callback callback = (metadata, exception) -> {
                    long latency = System.currentTimeMillis() - sendStartMs;
                    if (exception != null) {
                        errors.incrementAndGet();
                        return;
                    }
                    totalSent.incrementAndGet();
                    totalBytes.addAndGet(payloadSize);
                    totalLatency.addAndGet(latency);
                    long prevMax;
                    do {
                        prevMax = maxLatency.get();
                    } while (latency > prevMax && !maxLatency.compareAndSet(prevMax, latency));

                    // Sample latency
                    long idx = sampleIndex.getAndIncrement();
                    if (idx < sampleSize) {
                        latencySamples[(int) idx] = (int) latency;
                    }
                };

                switch (option) {
                    case 1:
                        // Option 1: single topic, event-id header only
                        ProducerRecord<String, byte[]> rec1 = new ProducerRecord<>(topicName, null, payload);
                        rec1.headers().add(new RecordHeader(EVENT_ID_HEADER, eventIdBytes));
                        producer.send(rec1, callback);
                        windowCount++;
                        windowBytes += payloadSize;
                        break;

                    case 2:
                        // Option 2: single topic, event-id header + mla-authz-bitmap
                        ProducerRecord<String, byte[]> rec2 = new ProducerRecord<>(topicName, null, payload);
                        rec2.headers().add(new RecordHeader(EVENT_ID_HEADER, eventIdBytes));
                        byte[] bitmap = bitmapCache.get(eventId);
                        if (bitmap != null) {
                            rec2.headers().add(new RecordHeader(MLA_AUTHZ_HEADER, bitmap));
                        }
                        producer.send(rec2, callback);
                        windowCount++;
                        windowBytes += payloadSize;
                        break;

                    case 3:
                        // Option 3: route to per-audience topics
                        String[] targets = EVENT_ID_TO_TOPICS.get(eventId);
                        if (targets != null) {
                            for (String target : targets) {
                                ProducerRecord<String, byte[]> rec3 = new ProducerRecord<>(target, null, payload);
                                rec3.headers().add(new RecordHeader(EVENT_ID_HEADER, eventIdBytes));
                                producer.send(rec3, callback);
                                windowCount++;
                                windowBytes += payloadSize;
                            }
                        }
                        break;
                }

                // Progress reporting
                long now = System.currentTimeMillis();
                if (now - windowStart >= reportIntervalMs) {
                    double elapsed = (now - windowStart) / 1000.0;
                    double recsPerSec = windowCount / elapsed;
                    double mbPerSec = windowBytes / elapsed / (1024.0 * 1024.0);
                    System.out.printf("[PRODUCER] %d records sent, %.0f records/sec, %.2f MB/sec%n",
                        totalSent.get(), recsPerSec, mbPerSec);
                    windowStart = now;
                    windowCount = 0;
                    windowBytes = 0;
                }
            }

            producer.flush();
        }

        long endMs = System.currentTimeMillis();
        double elapsedSec = (endMs - startMs) / 1000.0;
        long sent = totalSent.get();
        double recsPerSec = sent / elapsedSec;
        double mbPerSec = totalBytes.get() / elapsedSec / (1024.0 * 1024.0);
        double avgLatency = sent > 0 ? totalLatency.get() / (double) sent : 0;

        // Calculate percentiles
        int sampledCount = (int) Math.min(sampleIndex.get(), sampleSize);
        Arrays.sort(latencySamples, 0, sampledCount);
        int p50 = sampledCount > 0 ? latencySamples[(int) (sampledCount * 0.50)] : 0;
        int p95 = sampledCount > 0 ? latencySamples[(int) (sampledCount * 0.95)] : 0;
        int p99 = sampledCount > 0 ? latencySamples[(int) (sampledCount * 0.99)] : 0;
        int p999 = sampledCount > 0 ? latencySamples[(int) (sampledCount * 0.999)] : 0;

        System.out.println();
        System.out.println("=== PRODUCER RESULTS ===");
        System.out.printf("option=%d%n", option);
        System.out.printf("total_records=%d%n", sent);
        System.out.printf("total_bytes=%d%n", totalBytes.get());
        System.out.printf("elapsed_sec=%.2f%n", elapsedSec);
        System.out.printf("records_per_sec=%.0f%n", recsPerSec);
        System.out.printf("mb_per_sec=%.2f%n", mbPerSec);
        System.out.printf("avg_latency_ms=%.2f%n", avgLatency);
        System.out.printf("max_latency_ms=%d%n", maxLatency.get());
        System.out.printf("p50_latency_ms=%d%n", p50);
        System.out.printf("p95_latency_ms=%d%n", p95);
        System.out.printf("p99_latency_ms=%d%n", p99);
        System.out.printf("p999_latency_ms=%d%n", p999);
        System.out.printf("errors=%d%n", errors.get());
    }
}
