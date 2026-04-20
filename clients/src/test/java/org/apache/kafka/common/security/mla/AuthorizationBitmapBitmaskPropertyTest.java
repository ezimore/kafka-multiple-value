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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link AuthorizationBitmap#createBitmask(int)}.
 *
 * <p>Feature: message-level-authorization, Property 4: Consumer Bitmask Has Exactly One Bit Set</p>
 *
 * <p><b>Validates: Requirements 6.2</b></p>
 */
// Feature: message-level-authorization, Property 4: Consumer Bitmask Has Exactly One Bit Set
public class AuthorizationBitmapBitmaskPropertyTest {

    /**
     * Property 4: Consumer Bitmask Has Exactly One Bit Set.
     *
     * For any valid consumer ID N (0–1000), the generated consumer bitmask shall be a
     * byte array with exactly one bit set to 1 at position N (big-endian bit order),
     * and all other bits set to 0. The popcount of the entire bitmask must be exactly 1.
     */
    @Property(tries = 100)
    public void bitmaskHasExactlyOneBitSet(@ForAll @IntRange(min = 0, max = 1000) int consumerId) {
        byte[] bitmask = AuthorizationBitmap.createBitmask(consumerId);

        // Verify byte length: ceil((consumerId + 1) / 8)
        int expectedLength = (consumerId / 8) + 1;
        assertEquals(expectedLength, bitmask.length,
            "Bitmask byte length should be ceil((consumerId + 1) / 8) for consumerId=" + consumerId);

        // Verify the bit at position consumerId is set
        assertTrue(AuthorizationBitmap.getBit(bitmask, consumerId),
            "Bit at position " + consumerId + " should be set in the bitmask");

        // Verify popcount = 1: count all set bits across the entire bitmask
        int popcount = 0;
        for (byte b : bitmask) {
            popcount += Integer.bitCount(b & 0xFF);
        }
        assertEquals(1, popcount,
            "Bitmask should have exactly one bit set (popcount = 1), but found " + popcount);

        // Verify all other bit positions are 0
        int totalBits = bitmask.length * 8;
        for (int pos = 0; pos < totalBits; pos++) {
            if (pos != consumerId) {
                assertFalse(AuthorizationBitmap.getBit(bitmask, pos),
                    "Bit at position " + pos + " should NOT be set (only position " + consumerId + " should be set)");
            }
        }
    }
}
