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
package org.apache.kafka.server.record.mla;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.mla.AuthorizationBitmap;
import org.apache.kafka.common.security.mla.ConsumerIdRegistryClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MLAPlugin#apply(KafkaPrincipal, TopicPartition, Record)}.
 *
 * <p>Validates: Requirements 7.2, 7.3, 7.4, 7.5, 7.6, 10.3</p>
 */
public class MLAPluginApplyTest {

    private static final TopicPartition TP = new TopicPartition("test-topic", 0);
    private static final String CONSUMER_0_PRINCIPAL = "User:consumer0";
    private static final String CONSUMER_1_PRINCIPAL = "User:consumer1";
    private static final String UNKNOWN_PRINCIPAL = "User:unknown";

    private MLAPlugin plugin;
    private ConsumerIdRegistryClient mockRegistryClient;
    private ConcurrentMap<String, byte[]> bitmaskCache;

    @BeforeEach
    void setUp() throws Exception {
        plugin = new MLAPlugin();

        // Configure with defaults (strip enabled)
        plugin.configure(Map.of());

        // Create a mock registry client
        mockRegistryClient = mock(ConsumerIdRegistryClient.class);
        when(mockRegistryClient.getConsumerId(CONSUMER_0_PRINCIPAL)).thenReturn(0);
        when(mockRegistryClient.getConsumerId(CONSUMER_1_PRINCIPAL)).thenReturn(1);
        when(mockRegistryClient.getConsumerId(UNKNOWN_PRINCIPAL)).thenReturn(null);

        // Use reflection to inject the mock registry client
        Field registryClientField = MLAPlugin.class.getDeclaredField("registryClient");
        registryClientField.setAccessible(true);
        registryClientField.set(plugin, mockRegistryClient);

        // Pre-populate the bitmask cache with bitmasks for consumer 0 and consumer 1
        bitmaskCache = new ConcurrentHashMap<>();
        bitmaskCache.put(CONSUMER_0_PRINCIPAL, AuthorizationBitmap.createBitmask(0));
        bitmaskCache.put(CONSUMER_1_PRINCIPAL, AuthorizationBitmap.createBitmask(1));

        Field bitmaskCacheField = MLAPlugin.class.getDeclaredField("bitmaskCache");
        bitmaskCacheField.setAccessible(true);
        bitmaskCacheField.set(plugin, bitmaskCache);
    }

    /**
     * Test: Authorized record with strip enabled → header removed, data preserved.
     *
     * <p>When the authorization check result is non-zero and the MLAPlugin is configured
     * to strip the Authorization_Header, the plugin returns a new record with the
     * Authorization_Header removed.</p>
     *
     * <p><b>Validates: Requirements 7.2, 7.3</b></p>
     */
    @Test
    void testAuthorizedRecordWithStripEnabled() {
        // Create a bitmap authorizing consumer 0
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0));
        Header authzHeader = new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, bitmap);
        Header otherHeader = new RecordHeader("other-key", "other-value".getBytes());

        Record record = new MLAPluginPropertyTest.TestRecord(
            42L, 1000L,
            "test-key".getBytes(), "test-value".getBytes(),
            new Header[]{otherHeader, authzHeader}
        );

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "consumer0");

        Record result = plugin.apply(principal, TP, record);

        // Record should be returned (authorized)
        assertNotNull(result, "Authorized record should not be null");

        // Data should be preserved
        assertEquals(42L, result.offset());
        assertEquals(1000L, result.timestamp());
        assertEquals(ByteBuffer.wrap("test-key".getBytes()), result.key());
        assertEquals(ByteBuffer.wrap("test-value".getBytes()), result.value());

        // Authorization header should be stripped
        Header[] resultHeaders = result.headers();
        assertEquals(1, resultHeaders.length, "Should have only the non-authz header");
        assertEquals("other-key", resultHeaders[0].key());
        assertArrayEquals("other-value".getBytes(), resultHeaders[0].value());

        // Verify no mla-authz-bitmap header present
        for (Header h : resultHeaders) {
            assertTrue(!MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(h.key()),
                "Stripped record must not contain the mla-authz-bitmap header");
        }
    }

    /**
     * Test: Authorized record with strip disabled → original record returned.
     *
     * <p>When the authorization check result is non-zero and the MLAPlugin is configured
     * to retain the Authorization_Header, the plugin returns the reference to the
     * original record unchanged.</p>
     *
     * <p><b>Validates: Requirements 7.2, 7.4</b></p>
     */
    @Test
    void testAuthorizedRecordWithStripDisabled() throws Exception {
        // Reconfigure with strip disabled
        plugin.configure(Map.of(MLAPlugin.STRIP_HEADER_CONFIG, "false"));

        // Re-inject mock dependencies after reconfigure
        Field registryClientField = MLAPlugin.class.getDeclaredField("registryClient");
        registryClientField.setAccessible(true);
        registryClientField.set(plugin, mockRegistryClient);

        Field bitmaskCacheField = MLAPlugin.class.getDeclaredField("bitmaskCache");
        bitmaskCacheField.setAccessible(true);
        bitmaskCacheField.set(plugin, bitmaskCache);

        // Create a bitmap authorizing consumer 0
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0));
        Header authzHeader = new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, bitmap);

        Record record = new MLAPluginPropertyTest.TestRecord(
            10L, 2000L,
            "key".getBytes(), "value".getBytes(),
            new Header[]{authzHeader}
        );

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "consumer0");

        Record result = plugin.apply(principal, TP, record);

        // Should return the exact same record object (not a copy)
        assertSame(record, result, "With strip disabled, the original record reference should be returned");
    }

    /**
     * Test: Unauthorized record → null returned.
     *
     * <p>When the authorization check result is zero, the plugin returns null.</p>
     *
     * <p><b>Validates: Requirements 7.2, 7.5</b></p>
     */
    @Test
    void testUnauthorizedRecord() {
        // Create a bitmap authorizing only consumer 0
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0));
        Header authzHeader = new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, bitmap);

        Record record = new MLAPluginPropertyTest.TestRecord(
            5L, 3000L,
            "key".getBytes(), "value".getBytes(),
            new Header[]{authzHeader}
        );

        // Consumer 1 is NOT authorized for this record
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "consumer1");

        Record result = plugin.apply(principal, TP, record);

        assertNull(result, "Unauthorized record should return null");
    }

    /**
     * Test: Missing header → null returned.
     *
     * <p>If a record on an MLA_Enabled_Topic does not contain an Authorization_Header,
     * the plugin returns null.</p>
     *
     * <p><b>Validates: Requirements 7.6</b></p>
     */
    @Test
    void testMissingHeader() {
        // Record with no mla-authz-bitmap header
        Header otherHeader = new RecordHeader("some-header", "some-value".getBytes());

        Record record = new MLAPluginPropertyTest.TestRecord(
            1L, 4000L,
            "key".getBytes(), "value".getBytes(),
            new Header[]{otherHeader}
        );

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "consumer0");

        Record result = plugin.apply(principal, TP, record);

        assertNull(result, "Record without authorization header should return null");
    }

    /**
     * Test: Zero-length header → null returned.
     *
     * <p>If the Authorization_Header contains a zero-length byte array, the plugin
     * treats the record as unauthorized for all consumers.</p>
     *
     * <p><b>Validates: Requirements 10.3</b></p>
     */
    @Test
    void testZeroLengthHeader() {
        // Record with mla-authz-bitmap header containing empty byte array
        Header authzHeader = new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, new byte[0]);

        Record record = new MLAPluginPropertyTest.TestRecord(
            2L, 5000L,
            "key".getBytes(), "value".getBytes(),
            new Header[]{authzHeader}
        );

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "consumer0");

        Record result = plugin.apply(principal, TP, record);

        assertNull(result, "Record with zero-length authorization header should return null");
    }

    /**
     * Test: Principal not in registry → null returned.
     *
     * <p>When the consumer principal is not found in the Consumer ID Registry,
     * the plugin cannot generate a bitmask and returns null.</p>
     *
     * <p><b>Validates: Requirements 7.5</b></p>
     */
    @Test
    void testPrincipalNotInRegistry() {
        // Create a bitmap authorizing consumer 0
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0));
        Header authzHeader = new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, bitmap);

        Record record = new MLAPluginPropertyTest.TestRecord(
            3L, 6000L,
            "key".getBytes(), "value".getBytes(),
            new Header[]{authzHeader}
        );

        // Use a principal that is not in the registry
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unknown");

        Record result = plugin.apply(principal, TP, record);

        assertNull(result, "Record should return null when principal is not in registry");
    }
}
