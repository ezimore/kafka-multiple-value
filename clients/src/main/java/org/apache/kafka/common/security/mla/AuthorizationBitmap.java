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

import java.util.Set;

/**
 * Utility class for constructing and evaluating authorization bitmaps.
 * <p>
 * Uses <b>big-endian bit order</b>: bit position 0 corresponds to the most
 * significant bit (MSB) of byte 0.
 * <pre>
 * Byte index:    [0]        [1]        [2]       ...
 * Bit positions: 0 1 2 3    8 9 10 11  16 17 18 19
 *                4 5 6 7    12 13 14 15 20 21 22 23
 * </pre>
 * Bit position N is in byte {@code N / 8} at mask {@code 0x80 >>> (N % 8)}.
 */
public class AuthorizationBitmap {

    private AuthorizationBitmap() {
        // Utility class — not instantiable
    }

    /**
     * Create a bitmap with the specified consumer IDs authorized.
     * <p>
     * The returned byte array has the minimum length needed to represent all
     * IDs in the set. An empty set returns an empty byte array.
     *
     * @param authorizedIds the set of consumer IDs to authorize (must be non-negative)
     * @return the bitmap as a variable-length byte array
     */
    public static byte[] create(Set<Integer> authorizedIds) {
        if (authorizedIds.isEmpty()) {
            return new byte[0];
        }

        int maxId = -1;
        for (int id : authorizedIds) {
            if (id < 0) {
                throw new IllegalArgumentException("Consumer ID must be non-negative, got: " + id);
            }
            if (id > maxId) {
                maxId = id;
            }
        }

        byte[] bitmap = new byte[requiredBytes(maxId)];
        for (int id : authorizedIds) {
            setBit(bitmap, id);
        }
        return bitmap;
    }

    /**
     * Create a single-consumer bitmask for the given consumer ID.
     * <p>
     * The returned byte array has exactly one bit set to 1 at the position
     * corresponding to {@code consumerId}, using big-endian bit order.
     *
     * @param consumerId the consumer's ID (must be non-negative)
     * @return a byte array with only the bit at {@code consumerId} set to 1
     */
    public static byte[] createBitmask(int consumerId) {
        if (consumerId < 0) {
            throw new IllegalArgumentException("Consumer ID must be non-negative, got: " + consumerId);
        }

        byte[] bitmask = new byte[requiredBytes(consumerId)];
        setBit(bitmask, consumerId);
        return bitmask;
    }

    /**
     * Check if a consumer is authorized by performing a bitwise AND between
     * the authorization bitmap and the consumer bitmask.
     * <p>
     * When the authorization bitmap is shorter than the consumer bitmask, the
     * missing bytes are treated as {@code 0x00} (unauthorized). This means
     * consumers whose ID exceeds the bitmap's bit capacity are always
     * unauthorized.
     *
     * @param authorizationBitmap the record's authorization bitmap
     * @param consumerBitmask     the consumer's bitmask
     * @return true if the bitwise AND result is non-zero (consumer is authorized)
     */
    public static boolean isAuthorized(byte[] authorizationBitmap, byte[] consumerBitmask) {
        int minLen = Math.min(authorizationBitmap.length, consumerBitmask.length);
        for (int i = 0; i < minLen; i++) {
            if ((authorizationBitmap[i] & consumerBitmask[i]) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set a specific bit in a bitmap (big-endian bit order). The bitmap is
     * modified in place.
     *
     * @param bitmap      the bitmap byte array (modified in place)
     * @param bitPosition the bit position to set (0-based, must be within the array bounds)
     */
    public static void setBit(byte[] bitmap, int bitPosition) {
        int byteIndex = bitPosition / 8;
        int mask = 0x80 >>> (bitPosition % 8);
        bitmap[byteIndex] |= (byte) mask;
    }

    /**
     * Get the value of a specific bit in a bitmap (big-endian bit order).
     *
     * @param bitmap      the bitmap byte array
     * @param bitPosition the bit position to read (0-based, must be within the array bounds)
     * @return true if the bit is set
     */
    public static boolean getBit(byte[] bitmap, int bitPosition) {
        int byteIndex = bitPosition / 8;
        if (byteIndex >= bitmap.length) {
            return false;
        }
        int mask = 0x80 >>> (bitPosition % 8);
        return (bitmap[byteIndex] & mask) != 0;
    }

    /**
     * Compute the minimum byte array length needed to represent a bitmap
     * with the given maximum consumer ID.
     *
     * @param maxConsumerId the highest consumer ID to represent (must be non-negative)
     * @return the required byte array length
     */
    public static int requiredBytes(int maxConsumerId) {
        return (maxConsumerId / 8) + 1;
    }
}
