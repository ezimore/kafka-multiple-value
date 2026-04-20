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
package org.apache.kafka.common.security.mla;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConsumerIdRegistryClient}.
 *
 * <p>Tests exercise the cache behavior by invoking the private
 * {@code processRecord} method via reflection, without starting
 * the background consumer thread.</p>
 *
 * <p><b>Validates: Requirements 4.3, 4.4</b></p>
 */
public class ConsumerIdRegistryClientTest {

    private ConsumerIdRegistryClient client;
    private Method processRecordMethod;

    @BeforeEach
    public void setUp() throws Exception {
        // Create client without calling start() — no real Kafka connection needed
        client = new ConsumerIdRegistryClient("localhost:9092", "_consumer_id_registry");

        // Access the private processRecord method via reflection
        processRecordMethod = ConsumerIdRegistryClient.class.getDeclaredMethod(
                "processRecord", ConsumerRecord.class);
        processRecordMethod.setAccessible(true);
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    // ---------------------------------------------------------------
    // Helper: encode a consumer ID as a 4-byte big-endian byte array
    // ---------------------------------------------------------------

    private static byte[] encodeConsumerId(int consumerId) {
        return ByteBuffer.allocate(4).putInt(consumerId).array();
    }

    // ---------------------------------------------------------------
    // Helper: create a ConsumerRecord for the registry topic
    // ---------------------------------------------------------------

    private ConsumerRecord<String, byte[]> registryRecord(String key, byte[] value) {
        return new ConsumerRecord<>("_consumer_id_registry", 0, 0L, key, value);
    }

    // ---------------------------------------------------------------
    // Helper: invoke processRecord via reflection
    // ---------------------------------------------------------------

    private void invokeProcessRecord(ConsumerRecord<String, byte[]> record) throws Exception {
        processRecordMethod.invoke(client, record);
    }

    // ---------------------------------------------------------------
    // 1. Cache update on new record
    //    Requirement 4.3
    // ---------------------------------------------------------------

    @Test
    public void testCacheUpdateOnNewRecord() throws Exception {
        invokeProcessRecord(registryRecord("User:alice", encodeConsumerId(0)));

        Integer consumerId = client.getConsumerId("User:alice");
        assertNotNull(consumerId, "Consumer ID should be present after processing a record");
        assertEquals(0, consumerId, "Consumer ID for User:alice should be 0");
    }

    // ---------------------------------------------------------------
    // 2. Cache update on second record — both mappings present
    //    Requirement 4.3
    // ---------------------------------------------------------------

    @Test
    public void testCacheUpdateOnMultipleRecords() throws Exception {
        invokeProcessRecord(registryRecord("User:alice", encodeConsumerId(0)));
        invokeProcessRecord(registryRecord("User:bob", encodeConsumerId(1)));

        assertEquals(0, client.getConsumerId("User:alice"),
                "User:alice should have consumer ID 0");
        assertEquals(1, client.getConsumerId("User:bob"),
                "User:bob should have consumer ID 1");

        Map<String, Integer> mappings = client.getAllMappings();
        assertEquals(2, mappings.size(), "Should have 2 mappings");
    }

    // ---------------------------------------------------------------
    // 3. Cache removal on tombstone record
    //    Requirement 4.4
    // ---------------------------------------------------------------

    @Test
    public void testCacheRemovalOnTombstone() throws Exception {
        // First, add two principals
        invokeProcessRecord(registryRecord("User:alice", encodeConsumerId(0)));
        invokeProcessRecord(registryRecord("User:bob", encodeConsumerId(1)));
        assertEquals(2, client.getAllMappings().size(), "Should have 2 mappings before tombstone");

        // Tombstone for User:alice (null value)
        invokeProcessRecord(registryRecord("User:alice", null));

        assertNull(client.getConsumerId("User:alice"),
                "User:alice should be removed after tombstone");
        assertEquals(1, client.getConsumerId("User:bob"),
                "User:bob should still be present after User:alice tombstone");
        assertEquals(1, client.getAllMappings().size(),
                "Should have 1 mapping after tombstone");
    }

    // ---------------------------------------------------------------
    // 4. getConsumerId returns null for unknown principal
    //    Requirement 4.3
    // ---------------------------------------------------------------

    @Test
    public void testGetConsumerIdReturnsNullForUnknownPrincipal() {
        assertNull(client.getConsumerId("User:unknown"),
                "getConsumerId should return null for an unknown principal");
    }

    // ---------------------------------------------------------------
    // 5. getAllMappings returns correct snapshot
    //    Requirement 4.3
    // ---------------------------------------------------------------

    @Test
    public void testGetAllMappingsReturnsSnapshot() throws Exception {
        // Empty initially
        assertTrue(client.getAllMappings().isEmpty(),
                "getAllMappings should be empty initially");

        // Add entries
        invokeProcessRecord(registryRecord("User:alice", encodeConsumerId(0)));
        invokeProcessRecord(registryRecord("User:bob", encodeConsumerId(1)));
        invokeProcessRecord(registryRecord("User:charlie", encodeConsumerId(2)));

        Map<String, Integer> mappings = client.getAllMappings();
        assertEquals(3, mappings.size(), "Should have 3 mappings");
        assertEquals(0, mappings.get("User:alice"));
        assertEquals(1, mappings.get("User:bob"));
        assertEquals(2, mappings.get("User:charlie"));
    }

    // ---------------------------------------------------------------
    // 6. Null key record is ignored
    // ---------------------------------------------------------------

    @Test
    public void testNullKeyRecordIsIgnored() throws Exception {
        // Process a record with null key — should be silently ignored
        invokeProcessRecord(registryRecord(null, encodeConsumerId(0)));

        assertTrue(client.getAllMappings().isEmpty(),
                "Cache should remain empty after processing a null-key record");
    }

    // ---------------------------------------------------------------
    // 7. Wrong value length is ignored
    // ---------------------------------------------------------------

    @Test
    public void testWrongValueLengthIsIgnored() throws Exception {
        // Process a record with 3-byte value (expected 4 bytes)
        invokeProcessRecord(registryRecord("User:alice", new byte[]{0x00, 0x01, 0x02}));

        assertNull(client.getConsumerId("User:alice"),
                "Record with wrong value length should be ignored");
        assertTrue(client.getAllMappings().isEmpty(),
                "Cache should remain empty after processing a record with wrong value length");
    }
}
