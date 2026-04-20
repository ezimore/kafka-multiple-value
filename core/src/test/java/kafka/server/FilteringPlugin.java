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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.record.RecordFetchPlugin;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test-only RecordFetchPlugin that filters out records whose keys start with "skip-".
 * Records with keys starting with "keep-" (or any other prefix) are passed through unchanged.
 *
 * <p>This plugin is a public top-level class so the broker can load it via reflection.
 * Used by integration tests to verify offset advancement and watermark correctness
 * when records are filtered out by the plugin chain.</p>
 */
public class FilteringPlugin implements RecordFetchPlugin {

    /** Prefix for record keys that should be filtered out (excluded from fetch responses). */
    public static final String SKIP_PREFIX = "skip-";

    /**
     * Tracks the number of records filtered (excluded) per topic.
     */
    private static final ConcurrentHashMap<String, AtomicInteger> FILTERED_COUNTS = new ConcurrentHashMap<>();

    /**
     * Tracks the number of records passed through per topic.
     */
    private static final ConcurrentHashMap<String, AtomicInteger> PASSED_COUNTS = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        // no-op
    }

    @Override
    public void start(Authorizer authorizer) {
        // no-op
    }

    @Override
    public boolean isActiveForTopic(String topic, Map<String, String> topicConfig) {
        String pluginList = topicConfig.get("record.fetch.plugins");
        if (pluginList == null || pluginList.isEmpty()) {
            return false;
        }
        String thisClassName = this.getClass().getName();
        for (String className : pluginList.split(",")) {
            if (className.trim().equals(thisClassName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Record apply(KafkaPrincipal principal, TopicPartition topicPartition, Record record) {
        String topic = topicPartition.topic();
        if (record.hasKey()) {
            ByteBuffer keyBuffer = record.key().duplicate();
            String key = Utils.utf8(keyBuffer);
            if (key.startsWith(SKIP_PREFIX)) {
                FILTERED_COUNTS
                    .computeIfAbsent(topic, k -> new AtomicInteger(0))
                    .incrementAndGet();
                return null; // Filter out this record
            }
        }
        PASSED_COUNTS
            .computeIfAbsent(topic, k -> new AtomicInteger(0))
            .incrementAndGet();
        return record; // Pass through
    }

    @Override
    public void close() {
        // no-op
    }

    // ---- Static accessors for test assertions ----

    /** Get the number of records filtered out for the given topic. */
    public static int getFilteredCount(String topic) {
        AtomicInteger count = FILTERED_COUNTS.get(topic);
        return count == null ? 0 : count.get();
    }

    /** Get the number of records passed through for the given topic. */
    public static int getPassedCount(String topic) {
        AtomicInteger count = PASSED_COUNTS.get(topic);
        return count == null ? 0 : count.get();
    }

    /** Reset all counts. Call before each test. */
    public static void resetCounts() {
        FILTERED_COUNTS.clear();
        PASSED_COUNTS.clear();
    }
}
