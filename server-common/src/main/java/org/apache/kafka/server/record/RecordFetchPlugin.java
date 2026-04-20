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
package org.apache.kafka.server.record;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Authorizer;

import java.io.Closeable;
import java.util.Map;

/**
 * A broker-side plugin interface for intercepting records during fetch
 * response construction. Plugins can inspect, transform, or filter
 * individual records before they are delivered to consumers.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Instantiated via no-arg constructor at broker startup.</li>
 *   <li>{@link #configure(Map)} called with broker configuration.</li>
 *   <li>{@link #start(Authorizer)} called with the broker's authorizer instance.</li>
 *   <li>{@link #isActiveForTopic(String, Map)} called to determine per-topic activation.</li>
 *   <li>{@link #apply(KafkaPrincipal, TopicPartition, Record)} called for each record during fetch on active topics.</li>
 *   <li>{@link #close()} called at broker shutdown.</li>
 * </ol>
 */
public interface RecordFetchPlugin extends Configurable, Closeable {

    /**
     * Called after {@link #configure(Map)} to provide the broker's Authorizer instance.
     * Plugins that need ACL information for extensibility can use this reference.
     *
     * @param authorizer the broker's configured Authorizer, or null if none is configured
     */
    void start(Authorizer authorizer);

    /**
     * Determines whether this plugin is active for the given topic.
     * The plugin should check the topic's configuration (passed via
     * configure or a separate mechanism) to decide.
     *
     * @param topic the topic name
     * @param topicConfig the topic's configuration properties
     * @return true if this plugin should process records for this topic
     */
    boolean isActiveForTopic(String topic, Map<String, String> topicConfig);

    /**
     * Process a single record during fetch response construction.
     *
     * @param principal the authenticated consumer principal
     * @param topicPartition the topic-partition being fetched
     * @param record a read-only reference to the record
     * @return the record to include in the response (original or newly
     *         constructed), or null to exclude the record
     */
    Record apply(KafkaPrincipal principal, TopicPartition topicPartition, Record record);
}
