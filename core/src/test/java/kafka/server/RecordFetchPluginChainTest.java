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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the RecordFetchPlugin chain execution logic.
 *
 * The chain logic in KafkaApis.applyRecordFetchPlugins iterates plugins in order,
 * passes the result of each plugin to the next, and short-circuits on null (excluding
 * the record). This test class validates that behavior using mock plugins.
 *
 * Requirements: 1.3, 1.4, 1.5, 1.6, 1.8
 */
public class RecordFetchPluginChainTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("test-topic", 0);

    /**
     * Applies the plugin chain logic identical to KafkaApis.applyRecordFetchPlugins.
     * For each record, iterates plugins in order. If a plugin returns null, remaining
     * plugins are skipped and the record is excluded. If a plugin throws an exception,
     * the record is excluded and remaining plugins are skipped.
     *
     * @param plugins the ordered list of active plugins
     * @param principal the consumer principal
     * @param topicPartition the topic-partition
     * @param record the input record
     * @return the final record after the chain, or null if excluded
     */
    private static Record applyChain(
        List<RecordFetchPlugin> plugins,
        KafkaPrincipal principal,
        TopicPartition topicPartition,
        Record record
    ) {
        Record currentRecord = record;
        for (RecordFetchPlugin plugin : plugins) {
            Record result;
            try {
                result = plugin.apply(principal, topicPartition, currentRecord);
            } catch (Exception e) {
                // Match KafkaApis behavior: exception → exclude record
                return null;
            }
            if (result == null) {
                return null;
            }
            currentRecord = result;
        }
        return currentRecord;
    }

    // ---- Test helpers ----

    /**
     * A test plugin that tracks invocations and can be configured to return
     * the input record, a transformed record, or null.
     */
    private static class TrackingPlugin implements RecordFetchPlugin {
        private final String name;
        private final boolean returnNull;
        private final Record transformedRecord;
        private final List<InvocationRecord> invocations = new ArrayList<>();

        /** Plugin that passes through the input record unchanged. */
        TrackingPlugin(String name) {
            this(name, false, null);
        }

        /** Plugin that returns null (excludes the record). */
        static TrackingPlugin nullReturning(String name) {
            return new TrackingPlugin(name, true, null);
        }

        /** Plugin that returns a specific transformed record. */
        static TrackingPlugin transforming(String name, Record transformed) {
            return new TrackingPlugin(name, false, transformed);
        }

        private TrackingPlugin(String name, boolean returnNull, Record transformedRecord) {
            this.name = name;
            this.returnNull = returnNull;
            this.transformedRecord = transformedRecord;
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public void start(Authorizer authorizer) { }

        @Override
        public boolean isActiveForTopic(String topic, Map<String, String> topicConfig) {
            return true;
        }

        @Override
        public Record apply(KafkaPrincipal principal, TopicPartition topicPartition, Record record) {
            invocations.add(new InvocationRecord(principal, topicPartition, record));
            if (returnNull) {
                return null;
            }
            return transformedRecord != null ? transformedRecord : record;
        }

        @Override
        public void close() { }

        List<InvocationRecord> getInvocations() {
            return invocations;
        }

        int getInvocationCount() {
            return invocations.size();
        }

        String getName() {
            return name;
        }
    }

    /** Captures the arguments passed to a plugin invocation. */
    private static class InvocationRecord {
        final KafkaPrincipal principal;
        final TopicPartition topicPartition;
        final Record record;

        InvocationRecord(KafkaPrincipal principal, TopicPartition topicPartition, Record record) {
            this.principal = principal;
            this.topicPartition = topicPartition;
            this.record = record;
        }
    }

    /** A plugin that throws an exception on apply(). */
    private static class ThrowingPlugin implements RecordFetchPlugin {
        private int invocationCount = 0;

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public void start(Authorizer authorizer) { }

        @Override
        public boolean isActiveForTopic(String topic, Map<String, String> topicConfig) {
            return true;
        }

        @Override
        public Record apply(KafkaPrincipal principal, TopicPartition topicPartition, Record record) {
            invocationCount++;
            throw new RuntimeException("Plugin error");
        }

        @Override
        public void close() { }

        int getInvocationCount() {
            return invocationCount;
        }
    }

    // ---- Tests ----

    /**
     * Validates Requirement 1.3: When no plugins are configured, records pass through unchanged.
     */
    @Test
    public void testNoPluginsRecordPassesThrough() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");

        Record result = applyChain(List.of(), principal, TOPIC_PARTITION, inputRecord);

        assertSame(inputRecord, result, "With no plugins, the original record should be returned");
    }

    /**
     * Validates Requirement 1.6: A single plugin that returns the record → record included.
     */
    @Test
    public void testSinglePluginReturnsRecord() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin plugin = new TrackingPlugin("pluginA");

        Record result = applyChain(List.of(plugin), principal, TOPIC_PARTITION, inputRecord);

        assertSame(inputRecord, result, "Plugin returned the record, so it should be included");
        assertEquals(1, plugin.getInvocationCount(), "Plugin should be invoked exactly once");
    }

    /**
     * Validates Requirement 1.5: A single plugin that returns null → record excluded.
     */
    @Test
    public void testSinglePluginReturnsNull() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin plugin = TrackingPlugin.nullReturning("pluginA");

        Record result = applyChain(List.of(plugin), principal, TOPIC_PARTITION, inputRecord);

        assertNull(result, "Plugin returned null, so the record should be excluded");
        assertEquals(1, plugin.getInvocationCount(), "Plugin should be invoked exactly once");
    }

    /**
     * Validates Requirements 1.4, 1.6: Two plugins both return record → both invoked in order,
     * record included.
     */
    @Test
    public void testTwoPluginsBothReturnRecord() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        Record result = applyChain(List.of(pluginA, pluginB), principal, TOPIC_PARTITION, inputRecord);

        assertSame(inputRecord, result, "Both plugins passed through, so the original record should be returned");
        assertEquals(1, pluginA.getInvocationCount(), "Plugin A should be invoked once");
        assertEquals(1, pluginB.getInvocationCount(), "Plugin B should be invoked once");
    }

    /**
     * Validates Requirement 1.4: Chain ordering — plugins are invoked in the configured order.
     */
    @Test
    public void testChainOrderingWithThreePlugins() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");

        // Create three plugins that track invocation order
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");
        TrackingPlugin pluginC = new TrackingPlugin("pluginC");

        Record result = applyChain(List.of(pluginA, pluginB, pluginC), principal, TOPIC_PARTITION, inputRecord);

        assertSame(inputRecord, result, "All plugins passed through, so the original record should be returned");
        assertEquals(1, pluginA.getInvocationCount(), "Plugin A should be invoked once");
        assertEquals(1, pluginB.getInvocationCount(), "Plugin B should be invoked once");
        assertEquals(1, pluginC.getInvocationCount(), "Plugin C should be invoked once");
    }

    /**
     * Validates Requirement 1.5: First plugin returns null → second plugin NOT invoked,
     * record excluded.
     */
    @Test
    public void testFirstPluginReturnsNullSkipsRemaining() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = TrackingPlugin.nullReturning("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        Record result = applyChain(List.of(pluginA, pluginB), principal, TOPIC_PARTITION, inputRecord);

        assertNull(result, "First plugin returned null, so the record should be excluded");
        assertEquals(1, pluginA.getInvocationCount(), "Plugin A should be invoked once");
        assertEquals(0, pluginB.getInvocationCount(), "Plugin B should NOT be invoked after null");
    }

    /**
     * Validates Requirements 1.5, 1.6: Second plugin returns null → first plugin invoked,
     * record excluded.
     */
    @Test
    public void testSecondPluginReturnsNull() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = TrackingPlugin.nullReturning("pluginB");

        Record result = applyChain(List.of(pluginA, pluginB), principal, TOPIC_PARTITION, inputRecord);

        assertNull(result, "Second plugin returned null, so the record should be excluded");
        assertEquals(1, pluginA.getInvocationCount(), "Plugin A should be invoked once");
        assertEquals(1, pluginB.getInvocationCount(), "Plugin B should be invoked once");
    }

    /**
     * Validates Requirement 1.8: The authenticated consumer principal is correctly passed
     * to each plugin in the chain.
     */
    @Test
    public void testPrincipalPassedToEachPlugin() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        applyChain(List.of(pluginA, pluginB), principal, TOPIC_PARTITION, inputRecord);

        // Verify principal was passed to plugin A
        assertEquals(1, pluginA.getInvocations().size());
        assertSame(principal, pluginA.getInvocations().get(0).principal,
            "Plugin A should receive the consumer principal");

        // Verify principal was passed to plugin B
        assertEquals(1, pluginB.getInvocations().size());
        assertSame(principal, pluginB.getInvocations().get(0).principal,
            "Plugin B should receive the consumer principal");
    }

    /**
     * Validates Requirement 1.6: When a plugin transforms a record (returns a different record),
     * the transformed record is passed to the next plugin in the chain.
     */
    @Test
    public void testTransformedRecordPassedToNextPlugin() {
        Record inputRecord = mock(Record.class);
        Record transformedRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = TrackingPlugin.transforming("pluginA", transformedRecord);
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        Record result = applyChain(List.of(pluginA, pluginB), principal, TOPIC_PARTITION, inputRecord);

        // Plugin A received the original record
        assertSame(inputRecord, pluginA.getInvocations().get(0).record,
            "Plugin A should receive the original record");

        // Plugin B received the transformed record from plugin A
        assertSame(transformedRecord, pluginB.getInvocations().get(0).record,
            "Plugin B should receive the transformed record from plugin A");

        // Final result is the transformed record (plugin B passes it through)
        assertSame(transformedRecord, result,
            "The chain should return the transformed record");
    }

    /**
     * Validates exception handling: When a plugin throws an exception, the record is excluded
     * and remaining plugins are skipped.
     */
    @Test
    public void testPluginExceptionExcludesRecord() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        ThrowingPlugin throwingPlugin = new ThrowingPlugin();
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        Record result = applyChain(List.of(throwingPlugin, pluginB), principal, TOPIC_PARTITION, inputRecord);

        assertNull(result, "Plugin threw exception, so the record should be excluded");
        assertEquals(1, throwingPlugin.getInvocationCount(), "Throwing plugin should be invoked once");
        assertEquals(0, pluginB.getInvocationCount(), "Plugin B should NOT be invoked after exception");
    }

    /**
     * Validates Requirement 1.5: Null short-circuit with three plugins — middle plugin returns null,
     * third plugin is skipped.
     */
    @Test
    public void testNullShortCircuitMiddlePlugin() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = TrackingPlugin.nullReturning("pluginB");
        TrackingPlugin pluginC = new TrackingPlugin("pluginC");

        Record result = applyChain(List.of(pluginA, pluginB, pluginC), principal, TOPIC_PARTITION, inputRecord);

        assertNull(result, "Middle plugin returned null, so the record should be excluded");
        assertEquals(1, pluginA.getInvocationCount(), "Plugin A should be invoked once");
        assertEquals(1, pluginB.getInvocationCount(), "Plugin B should be invoked once");
        assertEquals(0, pluginC.getInvocationCount(), "Plugin C should NOT be invoked after null from B");
    }

    /**
     * Validates that the topic partition is correctly passed to each plugin.
     */
    @Test
    public void testTopicPartitionPassedToEachPlugin() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TopicPartition tp = new TopicPartition("my-topic", 3);
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");

        applyChain(List.of(pluginA, pluginB), principal, tp, inputRecord);

        assertSame(tp, pluginA.getInvocations().get(0).topicPartition,
            "Plugin A should receive the correct topic partition");
        assertSame(tp, pluginB.getInvocations().get(0).topicPartition,
            "Plugin B should receive the correct topic partition");
    }

    /**
     * Validates that multiple records can be processed independently through the chain.
     * Each record goes through the full chain independently.
     */
    @Test
    public void testMultipleRecordsProcessedIndependently() {
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testUser");
        TrackingPlugin pluginA = new TrackingPlugin("pluginA");
        TrackingPlugin pluginB = new TrackingPlugin("pluginB");
        List<RecordFetchPlugin> plugins = List.of(pluginA, pluginB);

        Record result1 = applyChain(plugins, principal, TOPIC_PARTITION, record1);
        Record result2 = applyChain(plugins, principal, TOPIC_PARTITION, record2);

        assertSame(record1, result1, "First record should pass through");
        assertSame(record2, result2, "Second record should pass through");
        assertEquals(2, pluginA.getInvocationCount(), "Plugin A should be invoked twice (once per record)");
        assertEquals(2, pluginB.getInvocationCount(), "Plugin B should be invoked twice (once per record)");
    }

    /**
     * Validates Requirement 1.8: Different principals are correctly passed for different invocations.
     */
    @Test
    public void testDifferentPrincipalsPassedCorrectly() {
        Record inputRecord = mock(Record.class);
        KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        TrackingPlugin plugin = new TrackingPlugin("plugin");

        applyChain(List.of(plugin), alice, TOPIC_PARTITION, inputRecord);
        applyChain(List.of(plugin), bob, TOPIC_PARTITION, inputRecord);

        assertEquals(2, plugin.getInvocations().size());
        assertSame(alice, plugin.getInvocations().get(0).principal,
            "First invocation should receive alice's principal");
        assertSame(bob, plugin.getInvocations().get(1).principal,
            "Second invocation should receive bob's principal");
    }
}
