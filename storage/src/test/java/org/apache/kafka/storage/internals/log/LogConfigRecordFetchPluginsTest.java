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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the record.fetch.plugins topic-level configuration
 * and the validateRecordFetchPlugins validation method in LogConfig.
 */
public class LogConfigRecordFetchPluginsTest {

    @Test
    public void testRecordFetchPluginsDefaultIsEmpty() {
        LogConfig config = new LogConfig(Map.of());
        assertTrue(config.recordFetchPlugins.isEmpty(),
            "Default record.fetch.plugins should be an empty list");
    }

    @Test
    public void testRecordFetchPluginsAcceptsSingleClass() {
        LogConfig config = new LogConfig(Map.of(
            TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, List.of("com.example.MyPlugin")
        ));
        assertEquals(List.of("com.example.MyPlugin"), config.recordFetchPlugins);
    }

    @Test
    public void testRecordFetchPluginsAcceptsMultipleClasses() {
        LogConfig config = new LogConfig(Map.of(
            TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, List.of("com.example.PluginA", "com.example.PluginB")
        ));
        assertEquals(List.of("com.example.PluginA", "com.example.PluginB"), config.recordFetchPlugins);
    }

    @Test
    public void testRecordFetchPluginsAcceptsEmptyList() {
        LogConfig config = new LogConfig(Map.of(
            TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, List.of()
        ));
        assertTrue(config.recordFetchPlugins.isEmpty());
    }

    @Test
    public void testValidateRecordFetchPluginsWithEmptyList() {
        assertDoesNotThrow(() ->
            LogConfig.validateRecordFetchPlugins(List.of(), Set.of("com.example.PluginA"))
        );
    }

    @Test
    public void testValidateRecordFetchPluginsWithNullList() {
        assertDoesNotThrow(() ->
            LogConfig.validateRecordFetchPlugins(null, Set.of("com.example.PluginA"))
        );
    }

    @Test
    public void testValidateRecordFetchPluginsWithLoadedClass() {
        assertDoesNotThrow(() ->
            LogConfig.validateRecordFetchPlugins(
                List.of("com.example.PluginA"),
                Set.of("com.example.PluginA", "com.example.PluginB")
            )
        );
    }

    @Test
    public void testValidateRecordFetchPluginsWithMultipleLoadedClasses() {
        assertDoesNotThrow(() ->
            LogConfig.validateRecordFetchPlugins(
                List.of("com.example.PluginA", "com.example.PluginB"),
                Set.of("com.example.PluginA", "com.example.PluginB")
            )
        );
    }

    @Test
    public void testValidateRecordFetchPluginsRejectsUnloadedClass() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () ->
            LogConfig.validateRecordFetchPlugins(
                List.of("com.example.UnknownPlugin"),
                Set.of("com.example.PluginA")
            )
        );
        assertTrue(ex.getMessage().contains("com.example.UnknownPlugin"),
            "Error message should contain the unloaded class name");
        assertTrue(ex.getMessage().contains("not loaded at the broker"),
            "Error message should indicate the class is not loaded");
    }

    @Test
    public void testValidateRecordFetchPluginsRejectsPartiallyUnloadedList() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () ->
            LogConfig.validateRecordFetchPlugins(
                List.of("com.example.PluginA", "com.example.UnknownPlugin"),
                Set.of("com.example.PluginA")
            )
        );
        assertTrue(ex.getMessage().contains("com.example.UnknownPlugin"));
    }

    @Test
    public void testValidateRecordFetchPluginsWithEmptyBrokerPlugins() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () ->
            LogConfig.validateRecordFetchPlugins(
                List.of("com.example.PluginA"),
                Set.of()
            )
        );
        assertTrue(ex.getMessage().contains("com.example.PluginA"));
    }

    @Test
    public void testValidateRecordFetchPluginsIgnoresBlankEntries() {
        assertDoesNotThrow(() ->
            LogConfig.validateRecordFetchPlugins(
                List.of("", "  "),
                Set.of()
            )
        );
    }

    @Test
    public void testRecordFetchPluginsIsUnmodifiable() {
        LogConfig config = new LogConfig(Map.of(
            TopicConfig.RECORD_FETCH_PLUGINS_CONFIG, List.of("com.example.PluginA")
        ));
        assertThrows(UnsupportedOperationException.class, () ->
            config.recordFetchPlugins.add("com.example.PluginB")
        );
    }
}
