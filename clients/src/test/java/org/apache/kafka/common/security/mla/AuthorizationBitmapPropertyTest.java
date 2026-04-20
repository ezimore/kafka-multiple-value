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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link AuthorizationBitmap}.
 *
 * <p>Feature: message-level-authorization, Property 3: Authorization Bitmap Construction Round-Trip</p>
 *
 * <p><b>Validates: Requirements 5.1, 5.2, 5.5, 10.1</b></p>
 */
public class AuthorizationBitmapPropertyTest {

    @Provide
    Arbitrary<Set<@IntRange(min = 0, max = 1000) Integer>> consumerIdSets() {
        return Arbitraries.integers()
            .between(0, 1000)
            .set()
            .ofMaxSize(200);
    }

    @Provide
    Arbitrary<byte[]> randomBitmaps() {
        return Arbitraries.integers()
            .between(1, 128)
            .flatMap(size -> Arbitraries.bytes()
                .array(byte[].class)
                .ofSize(size));
    }

    /**
     * Property 3: Authorization Bitmap Construction Round-Trip.
     *
     * For any set of non-negative consumer IDs, constructing an authorization bitmap
     * and then reading back each bit position returns true for every ID in the authorized
     * set and false for every ID not in the set. The bitmap byte length equals
     * {@code ceil((maxId + 1) / 8)}.
     */
    @Property(tries = 100)
    public void bitmapConstructionRoundTrip(@ForAll("consumerIdSets") Set<Integer> authorizedIds) {
        byte[] bitmap = AuthorizationBitmap.create(authorizedIds);

        if (authorizedIds.isEmpty()) {
            assertEquals(0, bitmap.length,
                "Empty ID set should produce a zero-length bitmap");
            return;
        }

        int maxId = authorizedIds.stream().mapToInt(Integer::intValue).max().orElseThrow();

        // Verify byte length: ceil((maxId + 1) / 8)
        int expectedLength = (maxId / 8) + 1;
        assertEquals(expectedLength, bitmap.length,
            "Bitmap byte length should be ceil((maxId + 1) / 8) for maxId=" + maxId);

        // Verify every authorized ID has its bit set
        for (int id : authorizedIds) {
            assertTrue(AuthorizationBitmap.getBit(bitmap, id),
                "Bit at position " + id + " should be set (ID is in the authorized set)");
        }

        // Verify every bit position NOT in the authorized set is unset,
        // checking all positions within the bitmap's capacity
        int totalBits = bitmap.length * 8;
        for (int pos = 0; pos < totalBits; pos++) {
            if (!authorizedIds.contains(pos)) {
                assertFalse(AuthorizationBitmap.getBit(bitmap, pos),
                    "Bit at position " + pos + " should NOT be set (ID is not in the authorized set)");
            }
        }
    }

    // Feature: message-level-authorization, Property 5: Authorization Check Correctness

    /**
     * Property 5: Authorization Check Correctness.
     *
     * <p>For any authorization bitmap B and consumer ID, the authorization check
     * {@code isAuthorized(B, createBitmask(id))} shall return true if and only if
     * the bit at position {@code id} is set in B. When the consumer ID exceeds the
     * bitmap's bit capacity (i.e., the bitmap is shorter than the bitmask), the
     * missing bytes are treated as 0x00, resulting in unauthorized.</p>
     *
     * <p><b>Validates: Requirements 7.2, 7.5, 10.4</b></p>
     */
    @Property(tries = 100)
    public void authorizationCheckCorrectness(
            @ForAll("randomBitmaps") byte[] bitmap,
            @ForAll @IntRange(min = 0, max = 1023) int consumerId) {

        byte[] bitmask = AuthorizationBitmap.createBitmask(consumerId);
        boolean authorized = AuthorizationBitmap.isAuthorized(bitmap, bitmask);
        boolean bitSet = AuthorizationBitmap.getBit(bitmap, consumerId);

        assertEquals(bitSet, authorized,
            "isAuthorized(bitmap, createBitmask(" + consumerId + ")) should return "
                + bitSet + " because getBit(bitmap, " + consumerId + ") = " + bitSet
                + " (bitmap length = " + bitmap.length + " bytes, "
                + (bitmap.length * 8) + " bits)");
    }

    /**
     * Property 5 (short-bitmap zero-padding): Authorization Check Correctness — Short Bitmap.
     *
     * <p>When the consumer ID exceeds the bitmap's bit capacity, the result shall
     * always be unauthorized because the missing bytes are treated as 0x00.</p>
     *
     * <p><b>Validates: Requirements 7.5, 10.4</b></p>
     */
    @Property(tries = 100)
    public void authorizationCheckShortBitmapZeroPadding(
            @ForAll("randomBitmaps") byte[] bitmap,
            @ForAll @IntRange(min = 0, max = 1023) int idOffset) {

        // Consumer ID that is beyond the bitmap's bit capacity
        int consumerId = bitmap.length * 8 + idOffset;
        byte[] bitmask = AuthorizationBitmap.createBitmask(consumerId);

        assertFalse(AuthorizationBitmap.isAuthorized(bitmap, bitmask),
            "Consumer ID " + consumerId + " exceeds bitmap capacity ("
                + (bitmap.length * 8) + " bits), should be unauthorized due to zero-padding");

        // Also verify getBit returns false for out-of-range positions
        assertFalse(AuthorizationBitmap.getBit(bitmap, consumerId),
            "getBit should return false for position " + consumerId
                + " beyond bitmap capacity (" + (bitmap.length * 8) + " bits)");
    }

    // Feature: message-level-authorization, Property 7: Producer-Plugin Bitmap Agreement (Round-Trip)

    /**
     * Property 7: Producer-Plugin Bitmap Agreement (Round-Trip).
     *
     * <p>For any set of authorized consumer IDs and for any consumer ID C,
     * constructing an authorization bitmap using the producer's bitmap utility
     * and evaluating it using the plugin's authorization check with a bitmask
     * for consumer C shall return authorized if and only if C is in the
     * authorized set.</p>
     *
     * <p><b>Validates: Requirements 10.2</b></p>
     */
    @Property(tries = 100)
    public void producerPluginBitmapAgreementRoundTrip(
            @ForAll("consumerIdSets") Set<Integer> authorizedIds,
            @ForAll @IntRange(min = 0, max = 1000) int queryId) {

        // Producer side: build the authorization bitmap from the authorized set
        byte[] bitmap = AuthorizationBitmap.create(authorizedIds);

        // Plugin side: build the consumer bitmask for the query consumer
        byte[] bitmask = AuthorizationBitmap.createBitmask(queryId);

        // Plugin side: perform the authorization check
        boolean authorized = AuthorizationBitmap.isAuthorized(bitmap, bitmask);

        // The round-trip must agree: authorized iff queryId is in the set
        boolean expected = authorizedIds.contains(queryId);
        assertEquals(expected, authorized,
            "isAuthorized(create(" + authorizedIds + "), createBitmask(" + queryId + ")) should be "
                + expected + " because queryId " + (expected ? "is" : "is not")
                + " in the authorized set");
    }
}
