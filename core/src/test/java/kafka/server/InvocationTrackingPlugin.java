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
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.record.RecordFetchPlugin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test-only RecordFetchPlugin that tracks per-topic invocations and activation.
 * Used by integration tests to verify that the plugin chain is invoked
 * only for topics with matching {@code record.fetch.plugins} configuration.
 *
 * <p>This plugin is a public top-level class so the broker can load it
 * via reflection at startup.
 */
public class InvocationTrackingPlugin implements RecordFetchPlugin {

    /**
     * Global invocation counts keyed by topic name (from apply() calls).
     * Shared across all instances (there is typically one per broker).
     */
    private static final ConcurrentHashMap<String, AtomicInteger> INVOCATION_COUNTS = new ConcurrentHashMap<>();

    /**
     * Counts of isActiveForTopic() returning true, keyed by topic name.
     */
    private static final ConcurrentHashMap<String, AtomicInteger> IS_ACTIVE_TRUE_COUNTS = new ConcurrentHashMap<>();

    private static volatile boolean configured = false;
    private static volatile boolean started = false;

    @Override
    public void configure(Map<String, ?> configs) {
        configured = true;
    }

    @Override
    public void start(Authorizer authorizer) {
        started = true;
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
                IS_ACTIVE_TRUE_COUNTS
                    .computeIfAbsent(topic, k -> new AtomicInteger(0))
                    .incrementAndGet();
                return true;
            }
        }
        return false;
    }

    @Override
    public Record apply(KafkaPrincipal principal, TopicPartition topicPartition, Record record) {
        INVOCATION_COUNTS
            .computeIfAbsent(topicPartition.topic(), k -> new AtomicInteger(0))
            .incrementAndGet();
        // Pass through all records unchanged
        return record;
    }

    @Override
    public void close() {
        // no-op
    }

    // ---- Static accessors for test assertions ----

    /** Get the number of times apply() was called for the given topic. */
    public static int getInvocationCount(String topic) {
        AtomicInteger count = INVOCATION_COUNTS.get(topic);
        return count == null ? 0 : count.get();
    }

    /** Get the number of times isActiveForTopic() returned true for the given topic. */
    public static int getIsActiveTrueCount(String topic) {
        AtomicInteger count = IS_ACTIVE_TRUE_COUNTS.get(topic);
        return count == null ? 0 : count.get();
    }

    /** Reset all invocation and activation counts. Call before each test. */
    public static void resetCounts() {
        INVOCATION_COUNTS.clear();
        IS_ACTIVE_TRUE_COUNTS.clear();
    }

    /** Whether configure() was called on any instance. */
    public static boolean isConfigured() {
        return configured;
    }

    /** Whether start() was called on any instance. */
    public static boolean isStarted() {
        return started;
    }

    /** Reset all static state. */
    public static void resetAll() {
        INVOCATION_COUNTS.clear();
        IS_ACTIVE_TRUE_COUNTS.clear();
        configured = false;
        started = false;
    }
}
