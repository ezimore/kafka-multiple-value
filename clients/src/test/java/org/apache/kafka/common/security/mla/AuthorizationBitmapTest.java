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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link AuthorizationBitmap}.
 *
 * <p><b>Validates: Requirements 5.3, 5.4, 10.3, 10.4</b></p>
 */
public class AuthorizationBitmapTest {

    // ---------------------------------------------------------------
    // 1. IDs {2, 5} produces bitmap 0x24 (single byte)
    //    Requirement 5.3
    // ---------------------------------------------------------------

    @Test
    public void testCreateBitmapForIds2And5() {
        byte[] bitmap = AuthorizationBitmap.create(Set.of(2, 5));

        assertEquals(1, bitmap.length, "Bitmap for IDs {2,5} should be 1 byte");
        assertEquals((byte) 0x24, bitmap[0],
            "IDs {2,5} → bit 2 (0x20) + bit 5 (0x04) = 0x24");
    }

    // ---------------------------------------------------------------
    // 2. ID {0} produces bitmap 0x80 (single byte)
    //    Requirement 5.4
    // ---------------------------------------------------------------

    @Test
    public void testCreateBitmapForId0() {
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0));

        assertEquals(1, bitmap.length, "Bitmap for ID {0} should be 1 byte");
        assertEquals((byte) 0x80, bitmap[0],
            "ID {0} → bit 0 = MSB of byte 0 = 0x80");
    }

    // ---------------------------------------------------------------
    // 3. Empty ID set produces zero-length bitmap
    //    Requirement 5.5 (variable-length), 10.3 (zero-length = unauthorized)
    // ---------------------------------------------------------------

    @Test
    public void testCreateBitmapForEmptyIdSet() {
        byte[] bitmap = AuthorizationBitmap.create(Collections.emptySet());

        assertEquals(0, bitmap.length, "Empty ID set should produce a zero-length bitmap");
    }

    // ---------------------------------------------------------------
    // 4. Zero-length bitmap: isAuthorized returns false for any consumer
    //    Requirement 10.3
    // ---------------------------------------------------------------

    @Test
    public void testZeroLengthBitmapIsUnauthorizedForAll() {
        byte[] emptyBitmap = new byte[0];

        // Consumer ID 0
        byte[] bitmask0 = AuthorizationBitmap.createBitmask(0);
        assertFalse(AuthorizationBitmap.isAuthorized(emptyBitmap, bitmask0),
            "Zero-length bitmap should be unauthorized for consumer 0");

        // Consumer ID 5
        byte[] bitmask5 = AuthorizationBitmap.createBitmask(5);
        assertFalse(AuthorizationBitmap.isAuthorized(emptyBitmap, bitmask5),
            "Zero-length bitmap should be unauthorized for consumer 5");

        // Consumer ID 100
        byte[] bitmask100 = AuthorizationBitmap.createBitmask(100);
        assertFalse(AuthorizationBitmap.isAuthorized(emptyBitmap, bitmask100),
            "Zero-length bitmap should be unauthorized for consumer 100");
    }

    // ---------------------------------------------------------------
    // 5. Padding behavior: bitmap shorter than bitmask → unauthorized
    //    Requirement 10.4
    // ---------------------------------------------------------------

    @Test
    public void testShortBitmapTreatedAsUnauthorized() {
        // Bitmap authorizes IDs {0, 2} → 1 byte: 0xA0
        byte[] bitmap = AuthorizationBitmap.create(Set.of(0, 2));
        assertEquals(1, bitmap.length, "Bitmap for IDs {0,2} should be 1 byte");

        // Consumer ID 8 requires byte index 1, which is beyond the bitmap
        byte[] bitmask8 = AuthorizationBitmap.createBitmask(8);
        assertFalse(AuthorizationBitmap.isAuthorized(bitmap, bitmask8),
            "Consumer 8 exceeds bitmap capacity (8 bits), should be unauthorized");

        // Consumer ID 15 requires byte index 1
        byte[] bitmask15 = AuthorizationBitmap.createBitmask(15);
        assertFalse(AuthorizationBitmap.isAuthorized(bitmap, bitmask15),
            "Consumer 15 exceeds bitmap capacity (8 bits), should be unauthorized");

        // Consumer ID 100 requires byte index 12
        byte[] bitmask100 = AuthorizationBitmap.createBitmask(100);
        assertFalse(AuthorizationBitmap.isAuthorized(bitmap, bitmask100),
            "Consumer 100 exceeds bitmap capacity (8 bits), should be unauthorized");
    }

    @Test
    public void testGetBitReturnsFalseForPositionBeyondBitmap() {
        byte[] bitmap = new byte[]{(byte) 0xFF}; // 1 byte, all bits set

        // Positions 0–7 are within the bitmap and should be true
        for (int i = 0; i < 8; i++) {
            assertTrue(AuthorizationBitmap.getBit(bitmap, i),
                "Bit " + i + " should be set in 0xFF bitmap");
        }

        // Positions 8+ are beyond the bitmap and should return false
        assertFalse(AuthorizationBitmap.getBit(bitmap, 8),
            "Bit 8 is beyond bitmap length, should return false");
        assertFalse(AuthorizationBitmap.getBit(bitmap, 16),
            "Bit 16 is beyond bitmap length, should return false");
    }

    // ---------------------------------------------------------------
    // 6. requiredBytes method correctness
    // ---------------------------------------------------------------

    @Test
    public void testRequiredBytes() {
        // Consumer ID 0 → byte index 0 → 1 byte needed
        assertEquals(1, AuthorizationBitmap.requiredBytes(0));

        // Consumer ID 7 → byte index 0 → 1 byte needed
        assertEquals(1, AuthorizationBitmap.requiredBytes(7));

        // Consumer ID 8 → byte index 1 → 2 bytes needed
        assertEquals(2, AuthorizationBitmap.requiredBytes(8));

        // Consumer ID 15 → byte index 1 → 2 bytes needed
        assertEquals(2, AuthorizationBitmap.requiredBytes(15));

        // Consumer ID 16 → byte index 2 → 3 bytes needed
        assertEquals(3, AuthorizationBitmap.requiredBytes(16));

        // Consumer ID 255 → byte index 31 → 32 bytes needed
        assertEquals(32, AuthorizationBitmap.requiredBytes(255));
    }

    // ---------------------------------------------------------------
    // 7. setBit and getBit round-trip for specific positions
    // ---------------------------------------------------------------

    @Test
    public void testSetBitAndGetBitRoundTrip() {
        // Test bit 0 (MSB of byte 0)
        byte[] bitmap = new byte[1];
        AuthorizationBitmap.setBit(bitmap, 0);
        assertTrue(AuthorizationBitmap.getBit(bitmap, 0), "Bit 0 should be set");
        assertArrayEquals(new byte[]{(byte) 0x80}, bitmap, "Setting bit 0 should produce 0x80");

        // Test bit 7 (LSB of byte 0)
        bitmap = new byte[1];
        AuthorizationBitmap.setBit(bitmap, 7);
        assertTrue(AuthorizationBitmap.getBit(bitmap, 7), "Bit 7 should be set");
        assertArrayEquals(new byte[]{(byte) 0x01}, bitmap, "Setting bit 7 should produce 0x01");

        // Test bit 8 (MSB of byte 1)
        bitmap = new byte[2];
        AuthorizationBitmap.setBit(bitmap, 8);
        assertTrue(AuthorizationBitmap.getBit(bitmap, 8), "Bit 8 should be set");
        assertFalse(AuthorizationBitmap.getBit(bitmap, 0), "Bit 0 should not be set");
        assertEquals((byte) 0x00, bitmap[0], "Byte 0 should be untouched");
        assertEquals((byte) 0x80, bitmap[1], "Setting bit 8 should set MSB of byte 1");

        // Test multiple bits in the same byte
        bitmap = new byte[1];
        AuthorizationBitmap.setBit(bitmap, 2);
        AuthorizationBitmap.setBit(bitmap, 5);
        assertTrue(AuthorizationBitmap.getBit(bitmap, 2), "Bit 2 should be set");
        assertTrue(AuthorizationBitmap.getBit(bitmap, 5), "Bit 5 should be set");
        assertFalse(AuthorizationBitmap.getBit(bitmap, 0), "Bit 0 should not be set");
        assertFalse(AuthorizationBitmap.getBit(bitmap, 3), "Bit 3 should not be set");
        assertArrayEquals(new byte[]{(byte) 0x24}, bitmap,
            "Setting bits 2 and 5 should produce 0x24");
    }

    @Test
    public void testSetBitIsIdempotent() {
        byte[] bitmap = new byte[1];
        AuthorizationBitmap.setBit(bitmap, 3);
        AuthorizationBitmap.setBit(bitmap, 3); // set again
        assertTrue(AuthorizationBitmap.getBit(bitmap, 3), "Bit 3 should still be set");
        assertEquals((byte) 0x10, bitmap[0], "Double-setting bit 3 should still produce 0x10");
    }

    // ---------------------------------------------------------------
    // Additional: createBitmask produces correct single-bit bitmask
    // ---------------------------------------------------------------

    @Test
    public void testCreateBitmaskSingleBit() {
        // Consumer ID 0 → byte[]{0x80}
        byte[] bitmask0 = AuthorizationBitmap.createBitmask(0);
        assertEquals(1, bitmask0.length);
        assertEquals((byte) 0x80, bitmask0[0]);

        // Consumer ID 5 → byte[]{0x04}
        byte[] bitmask5 = AuthorizationBitmap.createBitmask(5);
        assertEquals(1, bitmask5.length);
        assertEquals((byte) 0x04, bitmask5[0]);

        // Consumer ID 8 → byte[]{0x00, 0x80}
        byte[] bitmask8 = AuthorizationBitmap.createBitmask(8);
        assertEquals(2, bitmask8.length);
        assertEquals((byte) 0x00, bitmask8[0]);
        assertEquals((byte) 0x80, bitmask8[1]);
    }

    // ---------------------------------------------------------------
    // Additional: isAuthorized with matching bitmap and bitmask
    // ---------------------------------------------------------------

    @Test
    public void testIsAuthorizedWithMatchingBitmapAndBitmask() {
        byte[] bitmap = AuthorizationBitmap.create(Set.of(2, 5));
        byte[] bitmask2 = AuthorizationBitmap.createBitmask(2);
        byte[] bitmask5 = AuthorizationBitmap.createBitmask(5);
        byte[] bitmask3 = AuthorizationBitmap.createBitmask(3);

        assertTrue(AuthorizationBitmap.isAuthorized(bitmap, bitmask2),
            "Consumer 2 should be authorized");
        assertTrue(AuthorizationBitmap.isAuthorized(bitmap, bitmask5),
            "Consumer 5 should be authorized");
        assertFalse(AuthorizationBitmap.isAuthorized(bitmap, bitmask3),
            "Consumer 3 should not be authorized");
    }
}
