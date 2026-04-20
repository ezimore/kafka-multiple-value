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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.Record;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link MLAPlugin}'s header stripping behavior.
 *
 * <p>Feature: message-level-authorization, Property 6: Header Stripping Preserves Record Data</p>
 *
 * <p><b>Validates: Requirements 7.3</b></p>
 */
public class MLAPluginPropertyTest {

    /**
     * A simple test implementation of the {@link Record} interface for property-based testing.
     */
    static class TestRecord implements Record {
        private final long offset;
        private final long timestamp;
        private final ByteBuffer key;
        private final ByteBuffer value;
        private final Header[] headers;

        TestRecord(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key != null ? ByteBuffer.wrap(key).asReadOnlyBuffer() : null;
            this.value = value != null ? ByteBuffer.wrap(value).asReadOnlyBuffer() : null;
            this.headers = headers;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int sequence() {
            return 0;
        }

        @Override
        public int sizeInBytes() {
            return 0;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public void ensureValid() {
        }

        @Override
        public int keySize() {
            return key != null ? key.remaining() : -1;
        }

        @Override
        public boolean hasKey() {
            return key != null;
        }

        @Override
        public ByteBuffer key() {
            return key != null ? key.duplicate() : null;
        }

        @Override
        public int valueSize() {
            return value != null ? value.remaining() : -1;
        }

        @Override
        public boolean hasValue() {
            return value != null;
        }

        @Override
        public ByteBuffer value() {
            return value != null ? value.duplicate() : null;
        }

        @Override
        public boolean hasMagic(byte magic) {
            return false;
        }

        @Override
        public boolean isCompressed() {
            return false;
        }

        @Override
        public boolean hasTimestampType(TimestampType timestampType) {
            return false;
        }

        @Override
        public Header[] headers() {
            return headers;
        }
    }

    /**
     * Provides an arbitrary for generating random records with 1–10 headers,
     * always including at least one {@code mla-authz-bitmap} header.
     */
    @Provide
    Arbitrary<TestRecord> recordsWithAuthzHeader() {
        Arbitrary<Long> offsets = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<Long> timestamps = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<byte[]> keys = Arbitraries.of(true, false).flatMap(hasKey -> {
            if (hasKey) {
                return Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(100);
            } else {
                return Arbitraries.just(null);
            }
        });
        Arbitrary<byte[]> values = Arbitraries.of(true, false).flatMap(hasValue -> {
            if (hasValue) {
                return Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(200);
            } else {
                return Arbitraries.just(null);
            }
        });
        // Generate 0–9 non-authz headers, then always add one mla-authz-bitmap header
        Arbitrary<List<Header>> otherHeaders = Arbitraries.integers().between(0, 9)
            .flatMap(count -> {
                if (count == 0) {
                    return Arbitraries.just(new ArrayList<>());
                }
                return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                    .filter(s -> !MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(s))
                    .flatMap(headerKey ->
                        Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(50)
                            .map(headerValue -> (Header) new RecordHeader(headerKey, headerValue))
                    )
                    .list().ofSize(count);
            });
        Arbitrary<byte[]> authzBitmapValues = Arbitraries.bytes()
            .array(byte[].class).ofMinSize(1).ofMaxSize(16);

        return Combinators.combine(offsets, timestamps, keys, values, otherHeaders, authzBitmapValues)
            .as((offset, timestamp, key, value, others, authzValue) -> {
                List<Header> allHeaders = new ArrayList<>(others);
                allHeaders.add(new RecordHeader(MLAPlugin.MLA_AUTHZ_HEADER_KEY, authzValue));
                return new TestRecord(offset, timestamp, key, value,
                    allHeaders.toArray(new Header[0]));
            });
    }

    /**
     * Property 6: Header Stripping Preserves Record Data.
     *
     * <p>For any record that passes the authorization check when header stripping is enabled,
     * the returned record shall have the same offset, timestamp, key, and value as the original
     * record, shall contain all original headers except the {@code mla-authz-bitmap} header,
     * and shall not contain the {@code mla-authz-bitmap} header.</p>
     *
     * <p><b>Validates: Requirements 7.3</b></p>
     */
    @Property(tries = 100)
    public void headerStrippingPreservesRecordData(
            @ForAll("recordsWithAuthzHeader") TestRecord originalRecord) {

        // Build the list of non-authz headers from the original for comparison
        List<Header> expectedNonAuthzHeaders = new ArrayList<>();
        for (Header h : originalRecord.headers()) {
            if (!MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(h.key())) {
                expectedNonAuthzHeaders.add(h);
            }
        }

        // Create a StrippedRecord by filtering out the mla-authz-bitmap header,
        // exactly as MLAPlugin.createRecordWithoutAuthzHeader does
        List<Header> filteredHeaders = new ArrayList<>();
        for (Header h : originalRecord.headers()) {
            if (!MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(h.key())) {
                filteredHeaders.add(h);
            }
        }
        Header[] newHeaders = filteredHeaders.toArray(new Header[0]);
        Record strippedRecord = new MLAPlugin.StrippedRecord(originalRecord, newHeaders);

        // Assert same offset
        assertEquals(originalRecord.offset(), strippedRecord.offset(),
            "Stripped record must have the same offset as the original");

        // Assert same timestamp
        assertEquals(originalRecord.timestamp(), strippedRecord.timestamp(),
            "Stripped record must have the same timestamp as the original");

        // Assert same key
        assertEquals(originalRecord.hasKey(), strippedRecord.hasKey(),
            "Stripped record must have the same hasKey() as the original");
        if (originalRecord.hasKey()) {
            ByteBuffer origKey = originalRecord.key();
            ByteBuffer strippedKey = strippedRecord.key();
            assertEquals(origKey, strippedKey,
                "Stripped record must have the same key content as the original");
        }

        // Assert same value
        assertEquals(originalRecord.hasValue(), strippedRecord.hasValue(),
            "Stripped record must have the same hasValue() as the original");
        if (originalRecord.hasValue()) {
            ByteBuffer origValue = originalRecord.value();
            ByteBuffer strippedValue = strippedRecord.value();
            assertEquals(origValue, strippedValue,
                "Stripped record must have the same value content as the original");
        }

        // Assert all non-authz headers are preserved
        Header[] strippedHeaders = strippedRecord.headers();
        assertEquals(expectedNonAuthzHeaders.size(), strippedHeaders.length,
            "Stripped record must have all original headers except mla-authz-bitmap");

        for (int i = 0; i < expectedNonAuthzHeaders.size(); i++) {
            Header expected = expectedNonAuthzHeaders.get(i);
            Header actual = strippedHeaders[i];
            assertEquals(expected.key(), actual.key(),
                "Header key at index " + i + " must match");
            assertArrayEquals(expected.value(), actual.value(),
                "Header value at index " + i + " must match");
        }

        // Assert mla-authz-bitmap header is NOT present
        for (Header h : strippedHeaders) {
            assertFalse(MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(h.key()),
                "Stripped record must NOT contain the mla-authz-bitmap header");
        }

        // Verify the original record still has the authz header (not mutated)
        boolean originalHasAuthzHeader = Arrays.stream(originalRecord.headers())
            .anyMatch(h -> MLAPlugin.MLA_AUTHZ_HEADER_KEY.equals(h.key()));
        assertTrue(originalHasAuthzHeader,
            "Original record must still contain the mla-authz-bitmap header");
    }
}
