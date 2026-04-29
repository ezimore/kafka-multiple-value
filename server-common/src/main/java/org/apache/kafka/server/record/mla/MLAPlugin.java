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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.mla.AuthorizationBitmap;
import org.apache.kafka.common.security.mla.ConsumerIdRegistryClient;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.record.RecordFetchPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Message-Level Authorization plugin implementation.
 * <p>
 * Filters records based on authorization bitmaps embedded in record headers.
 * Each record on an MLA-enabled topic carries an {@code mla-authz-bitmap} header
 * whose value is a variable-length byte array encoding which consumers are
 * authorized to receive the record. The plugin performs a bitwise AND between
 * the consumer's bitmask and the record's authorization bitmap; a non-zero
 * result means the consumer is authorized.
 * <p>
 * The plugin reads consumer ID mappings from the Consumer ID Registry topic
 * via a {@link ConsumerIdRegistryClient} and caches per-consumer bitmasks
 * for efficient repeated authorization checks.
 */
public class MLAPlugin implements RecordFetchPlugin {

    private static final Logger log = LoggerFactory.getLogger(MLAPlugin.class);

    /** Header key for the authorization bitmap attached to each record. */
    public static final String MLA_AUTHZ_HEADER_KEY = "mla-authz-bitmap";

    /** Configuration key controlling whether the authorization header is stripped from delivered records. */
    public static final String STRIP_HEADER_CONFIG = "mla.strip.authorization.header";

    /** Default value for {@link #STRIP_HEADER_CONFIG}. */
    public static final boolean STRIP_HEADER_DEFAULT = true;

    /** Configuration key for the Consumer ID Registry topic name. */
    public static final String CONSUMER_ID_REGISTRY_TOPIC_CONFIG = "mla.consumer.id.registry.topic";

    /** Default value for {@link #CONSUMER_ID_REGISTRY_TOPIC_CONFIG}. */
    public static final String CONSUMER_ID_REGISTRY_TOPIC_DEFAULT = "_consumer_id_registry";

    // State
    private boolean stripHeader;
    private String registryTopic;
    private String bootstrapServers;
    private ConsumerIdRegistryClient registryClient;
    private final ConcurrentMap<String, byte[]> bitmaskCache = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        Object stripObj = configs.get(STRIP_HEADER_CONFIG);
        if (stripObj != null) {
            stripHeader = Boolean.parseBoolean(stripObj.toString());
        } else {
            stripHeader = STRIP_HEADER_DEFAULT;
        }

        Object topicObj = configs.get(CONSUMER_ID_REGISTRY_TOPIC_CONFIG);
        if (topicObj != null) {
            registryTopic = topicObj.toString();
        } else {
            registryTopic = CONSUMER_ID_REGISTRY_TOPIC_DEFAULT;
        }

        // Try explicit bootstrap.servers first, then fall back to
        // advertised.listeners or listeners (KRaft brokers don't set bootstrap.servers).
        bootstrapServers = resolveBootstrapServers(configs);

        log.info("MLAPlugin configured: stripHeader={}, registryTopic={}", stripHeader, registryTopic);
    }

    @Override
    public void start(Authorizer authorizer) {
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot start MLAPlugin: bootstrap.servers not found in broker configuration. "
                    + "Checked: bootstrap.servers, advertised.listeners, listeners");
        }

        // Start the registry client in a background thread so it doesn't block
        // broker startup. The registry topic may not exist yet at startup time,
        // and ConsumerIdRegistryClient.start() calls partitionsFor() which blocks
        // until metadata is available.
        registryClient = new ConsumerIdRegistryClient(bootstrapServers, registryTopic);
        Thread registryStartThread = new Thread(() -> {
            int attempt = 0;
            while (true) {
                try {
                    attempt++;
                    registryClient.start();
                    log.info("MLAPlugin registry client connected to topic '{}' (attempt {})",
                            registryTopic, attempt);
                    return;
                } catch (Exception e) {
                    if (attempt <= 3) {
                        log.warn("MLAPlugin registry client failed to start (attempt {}), "
                                + "retrying in 5 seconds: {}", attempt, e.getMessage());
                    } else {
                        log.warn("MLAPlugin registry client failed to start (attempt {}), "
                                + "retrying in 5 seconds. The registry topic '{}' may not exist yet. "
                                + "Create it with: kafka-topics.sh --create --topic {} --config cleanup.policy=compact",
                                attempt, registryTopic, registryTopic);
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.info("MLAPlugin registry client startup interrupted");
                        return;
                    }
                }
            }
        }, "mla-registry-client-starter");
        registryStartThread.setDaemon(true);
        registryStartThread.start();

        log.info("MLAPlugin started with registry topic '{}' and bootstrap servers '{}' "
                + "(registry client connecting in background)",
                registryTopic, bootstrapServers);
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
        // Step 1: Read the mla-authz-bitmap header from the record
        byte[] authzBitmap = null;
        Header[] headers = record.headers();
        for (Header header : headers) {
            if (MLA_AUTHZ_HEADER_KEY.equals(header.key())) {
                authzBitmap = header.value();
                break;
            }
        }

        // If header is missing or zero-length, the record is unauthorized for all consumers
        if (authzBitmap == null || authzBitmap.length == 0) {
            return null;
        }

        // Step 2: Look up the consumer's bitmask from cache
        String principalName = principal.toString();
        byte[] consumerBitmask = getOrCreateBitmask(principalName);
        if (consumerBitmask == null) {
            // Principal not in registry — unauthorized
            return null;
        }

        // Step 3: Perform the authorization check
        if (!AuthorizationBitmap.isAuthorized(authzBitmap, consumerBitmask)) {
            return null;
        }

        // Step 4: Consumer is authorized
        if (stripHeader) {
            // Construct a new record without the mla-authz-bitmap header
            return createRecordWithoutAuthzHeader(record, headers);
        } else {
            // Return the original record unchanged
            return record;
        }
    }

    @Override
    public void close() {
        if (registryClient != null) {
            registryClient.close();
            registryClient = null;
        }
        bitmaskCache.clear();
        log.info("MLAPlugin closed");
    }

    /**
     * Resolve bootstrap servers from the broker configuration.
     * Tries, in order:
     * <ol>
     *   <li>{@code bootstrap.servers} (explicit override)</li>
     *   <li>{@code advertised.listeners} (KRaft / standard broker config)</li>
     *   <li>{@code listeners} (fallback)</li>
     * </ol>
     * For listener strings, only PLAINTEXT and SASL_PLAINTEXT endpoints are used,
     * and the {@code CONTROLLER://} listener is excluded.
     */
    private static String resolveBootstrapServers(Map<String, ?> configs) {
        // 1. Explicit bootstrap.servers
        Object bootstrapObj = configs.get("bootstrap.servers");
        if (bootstrapObj != null) {
            String val;
            if (bootstrapObj instanceof List<?> list) {
                val = String.join(",", list.stream().map(Object::toString).toList());
            } else {
                val = bootstrapObj.toString();
            }
            if (!val.isBlank()) {
                return val;
            }
        }

        // 2. advertised.listeners
        String result = extractFromListeners(configs.get("advertised.listeners"));
        if (result != null) {
            return result;
        }

        // 3. listeners
        return extractFromListeners(configs.get("listeners"));
    }

    /**
     * Extract host:port pairs from a Kafka listener string, filtering out
     * CONTROLLER listeners and stripping the protocol prefix.
     * Example input: {@code PLAINTEXT://:9092,CONTROLLER://:9093}
     * Example output: {@code localhost:9092}
     */
    private static String extractFromListeners(Object listenersObj) {
        if (listenersObj == null) {
            return null;
        }
        String listeners = listenersObj.toString();
        if (listeners.isBlank()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String part : listeners.split(",")) {
            String trimmed = part.trim();
            // Skip controller and SASL listeners (registry client has no SASL credentials)
            String upper = trimmed.toUpperCase();
            if (upper.startsWith("CONTROLLER") || upper.startsWith("SASL")) {
                continue;
            }
            // Strip protocol prefix: "PLAINTEXT://host:port" -> "host:port"
            int slashSlash = trimmed.indexOf("://");
            String hostPort = (slashSlash >= 0) ? trimmed.substring(slashSlash + 3) : trimmed;
            // Replace empty host (e.g., ":9092") with localhost
            if (hostPort.startsWith(":")) {
                hostPort = "localhost" + hostPort;
            }
            if (!hostPort.isBlank()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(hostPort);
            }
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    /**
     * Get or create a bitmask for the given principal. Looks up the consumer ID
     * from the registry client and generates the bitmask using
     * {@link AuthorizationBitmap#createBitmask(int)}.
     *
     * @param principalName the principal name (e.g., "User:alice")
     * @return the consumer bitmask, or null if the principal is not in the registry
     */
    private byte[] getOrCreateBitmask(String principalName) {
        // Registry client may not be connected yet (background startup)
        ConsumerIdRegistryClient client = registryClient;
        if (client == null || client.getConsumerId(principalName) == null) {
            bitmaskCache.remove(principalName);
            return null;
        }

        // Always check the registry client first to handle deletions.
        Integer consumerId = client.getConsumerId(principalName);
        if (consumerId == null) {
            bitmaskCache.remove(principalName);
            return null;
        }

        byte[] cached = bitmaskCache.get(principalName);
        if (cached != null) {
            return cached;
        }

        // Generate and cache the bitmask
        byte[] bitmask = AuthorizationBitmap.createBitmask(consumerId);
        bitmaskCache.put(principalName, bitmask);
        return bitmask;
    }

    /**
     * Create a new Record that has all the same fields as the original but
     * without the {@code mla-authz-bitmap} header.
     */
    private Record createRecordWithoutAuthzHeader(Record original, Header[] originalHeaders) {
        List<Header> filteredHeaders = new ArrayList<>(originalHeaders.length);
        for (Header header : originalHeaders) {
            if (!MLA_AUTHZ_HEADER_KEY.equals(header.key())) {
                filteredHeaders.add(header);
            }
        }
        Header[] newHeaders = filteredHeaders.toArray(new Header[0]);
        return new StrippedRecord(original, newHeaders);
    }

    /**
     * A Record wrapper that delegates all methods to the original record
     * except for {@link #headers()}, which returns the filtered header array.
     */
    static class StrippedRecord implements Record {

        private final Record delegate;
        private final Header[] headers;

        StrippedRecord(Record delegate, Header[] headers) {
            this.delegate = delegate;
            this.headers = headers;
        }

        @Override
        public long offset() {
            return delegate.offset();
        }

        @Override
        public int sequence() {
            return delegate.sequence();
        }

        @Override
        public int sizeInBytes() {
            return delegate.sizeInBytes();
        }

        @Override
        public long timestamp() {
            return delegate.timestamp();
        }

        @Override
        public void ensureValid() {
            delegate.ensureValid();
        }

        @Override
        public int keySize() {
            return delegate.keySize();
        }

        @Override
        public boolean hasKey() {
            return delegate.hasKey();
        }

        @Override
        public ByteBuffer key() {
            return delegate.key();
        }

        @Override
        public int valueSize() {
            return delegate.valueSize();
        }

        @Override
        public boolean hasValue() {
            return delegate.hasValue();
        }

        @Override
        public ByteBuffer value() {
            return delegate.value();
        }

        @Override
        public boolean hasMagic(byte magic) {
            return delegate.hasMagic(magic);
        }

        @Override
        public boolean isCompressed() {
            return delegate.isCompressed();
        }

        @Override
        public boolean hasTimestampType(TimestampType timestampType) {
            return delegate.hasTimestampType(timestampType);
        }

        @Override
        public Header[] headers() {
            return headers;
        }
    }
}
