# Implementation Plan: Message-Level Authorization

## Overview

This plan implements the Message-Level Authorization (MLA) feature for Apache Kafka. The work proceeds bottom-up: shared utilities first, then the broker-side plugin interface, the MLA plugin implementation, configuration extensions, test-only components, and finally integration and end-to-end tests. Each task builds on the previous ones so there is no orphaned code.

## Tasks

- [x] 1. Implement AuthorizationBitmap utility class
  - [x] 1.1 Create `AuthorizationBitmap` in `org.apache.kafka.common.security.mla`
    - Implement `create(Set<Integer>)` — builds a variable-length byte array with big-endian bit order
    - Implement `createBitmask(int)` — single-consumer bitmask with one bit set
    - Implement `isAuthorized(byte[], byte[])` — bitwise AND with short-array zero-padding
    - Implement `setBit(byte[], int)`, `getBit(byte[], int)`, `requiredBytes(int)`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 10.1, 10.4_

  - [x] 1.2 Write property test for bitmap construction round-trip (P3)
    - **Property 3: Authorization Bitmap Construction Round-Trip**
    - Generate random sets of consumer IDs (0–1000, set size 0–200)
    - Assert `getBit(create(ids), id)` returns true iff `id ∈ ids`; byte length = `ceil((max+1)/8)`
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 5.1, 5.2, 5.5, 10.1**

  - [x] 1.3 Write property test for single-bit bitmask (P4)
    - **Property 4: Consumer Bitmask Has Exactly One Bit Set**
    - Generate random consumer IDs (0–1000)
    - Assert bitmask has exactly one bit set at position N, popcount = 1
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 6.2**

  - [x] 1.4 Write property test for authorization check correctness (P5)
    - **Property 5: Authorization Check Correctness**
    - Generate random bitmaps (1–128 bytes) + random consumer IDs
    - Assert `isAuthorized(bitmap, bitmask(id))` iff bit at position `id` is set in bitmap
    - Verify short-bitmap zero-padding behavior
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 7.2, 7.5, 10.4**

  - [x] 1.5 Write property test for producer-plugin round-trip (P7)
    - **Property 7: Producer-Plugin Bitmap Agreement (Round-Trip)**
    - Generate random authorized ID sets + random query ID
    - Assert `isAuthorized(create(ids), bitmask(queryId))` iff `queryId ∈ ids`
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 10.2**

  - [x] 1.6 Write unit tests for AuthorizationBitmap
    - Test concrete examples: IDs {2,5} → `0x24`, ID {0} → `0x80`
    - Test zero-length bitmap, empty ID set
    - Test padding behavior when bitmap shorter than bitmask
    - _Requirements: 5.3, 5.4, 10.3, 10.4_

- [ ] 2. Implement ConsumerIdRegistryClient
  - [x] 2.1 Create `ConsumerIdRegistryClient` in `org.apache.kafka.common.security.mla`
    - Implement constructor accepting bootstrap servers and registry topic name
    - Implement `start()` — background thread consuming the registry topic from beginning
    - Implement `getConsumerId(String)` — lookup by principal, return null if not found
    - Implement `getAllMappings()` — return all current principal-to-ID mappings
    - Implement `close()` — shut down background consumer
    - Handle tombstone records by removing the principal from the cache
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 6.1_

  - [x] 2.2 Write unit tests for ConsumerIdRegistryClient
    - Test cache update on new record
    - Test cache removal on tombstone record
    - Test `getConsumerId` returns null for unknown principal
    - _Requirements: 4.3, 4.4_

- [ ] 3. Implement RecordFetchPlugin interface and broker-level configuration
  - [x] 3.1 Create `RecordFetchPlugin` interface in `org.apache.kafka.server.record`
    - Define `start(Authorizer)` method
    - Define `isActiveForTopic(String, Map<String, String>)` method
    - Define `apply(KafkaPrincipal, TopicPartition, Record)` method
    - Extend `Configurable` and `Closeable`
    - _Requirements: 1.1, 1.7, 1.8, 1.9_

  - [x] 3.2 Add broker-level `record.fetch.plugin.classes` configuration
    - Add config definition to broker configuration (e.g., `KafkaConfig`)
    - Default to empty string (no plugins)
    - Load and instantiate plugin classes at broker startup
    - Call `configure()` then `start(authorizer)` on each plugin instance
    - Call `close()` on each plugin at broker shutdown
    - _Requirements: 1.2, 1.3_

  - [x] 3.3 Add topic-level `record.fetch.plugins` configuration
    - Add config property to `TopicConfig.java` and `LogConfig.java`
    - Validate that referenced plugin classes are loaded at the broker during topic creation
    - Reject topic creation with `InvalidConfigurationException` if a class is not loaded
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 3.4 Integrate RecordFetchPlugin chain into the fetch path
    - In `KafkaApis.handleFetchRequest` (processResponseCallback), after records are read from the log:
      - Check topic config for active plugins via `isActiveForTopic()`
      - For topics with active plugins, iterate records and invoke the plugin chain
      - If a plugin returns null, skip remaining plugins and exclude the record
      - If a plugin returns a record, pass it to the next plugin
      - Build filtered `MemoryRecords` from the surviving records
    - Ensure fetch position advances past filtered records
    - Ensure high watermark and log start offset are correct in responses
    - _Requirements: 1.4, 1.5, 1.6, 9.1, 9.2, 11.1, 11.2, 11.3_

  - [x] 3.5 Write unit tests for plugin chain execution
    - Test chain ordering with 2–3 mock plugins
    - Test null short-circuit behavior (first plugin returns null → remaining skipped)
    - Test principal passthrough to each plugin
    - Test no-plugin path (records pass through unchanged)
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 1.8_

  - [x] 3.6 Write unit tests for topic configuration validation
    - Test valid plugin list accepted
    - Test empty list accepted (no plugins)
    - Test invalid/unloaded class name rejected with `InvalidConfigurationException`
    - _Requirements: 2.1, 2.2, 2.3_

- [x] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement MLAPlugin
  - [x] 5.1 Create `MLAPlugin` in `org.apache.kafka.server.record.mla`
    - Implement `RecordFetchPlugin` interface
    - In `configure()`: read `mla.strip.authorization.header` (default true) and `mla.consumer.id.registry.topic` (default `_consumer_id_registry`)
    - In `start()`: initialize `ConsumerIdRegistryClient`, start background consumer, build bitmask cache
    - In `isActiveForTopic()`: check if this plugin's class name is in the topic's `record.fetch.plugins` config
    - In `apply()`:
      - Read `mla-authz-bitmap` header; return null if missing or zero-length
      - Look up consumer bitmask from cache; return null if principal not in registry
      - Perform `AuthorizationBitmap.isAuthorized(bitmap, bitmask)`; return null if unauthorized
      - If authorized and strip enabled: construct new record without the `mla-authz-bitmap` header
      - If authorized and strip disabled: return original record
    - In `close()`: shut down `ConsumerIdRegistryClient`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 10.3_

  - [x] 5.2 Write property test for header stripping preserves record data (P6)
    - **Property 6: Header Stripping Preserves Record Data**
    - Generate random records with 1–10 headers including `mla-authz-bitmap`
    - Assert stripped record has same key/value/offset/timestamp, all headers except `mla-authz-bitmap`
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 7.3**

  - [x] 5.3 Write unit tests for MLAPlugin.apply()
    - Test authorized record with strip enabled → header removed, data preserved
    - Test authorized record with strip disabled → original record returned
    - Test unauthorized record → null returned
    - Test missing header → null returned
    - Test zero-length header → null returned
    - Test principal not in registry → null returned
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6, 10.3_

- [ ] 6. Implement test-only components
  - [x] 6.1 Create `TestAdminClient` in `org.apache.kafka.server.record.mla.test` (under `src/test/`)
    - Implement `createRegistryTopic(String, int)` — create compacted topic
    - Implement `registerConsumer(String)` — assign next auto-incremented Consumer ID, write to registry
    - Implement `deleteConsumer(String)` — write tombstone, track retired IDs
    - Implement `getNextConsumerId()` and `getRetiredIds()` for assertions
    - Enforce sequential ID assignment and never-reuse invariant
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x] 6.2 Write property test for sequential and unique ID assignment (P1)
    - **Property 1: Consumer ID Assignment is Sequential and Unique**
    - Generate random lists of 1–1000 unique principal strings
    - Assert IDs are 0..N-1, all distinct
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 3.3, 3.4**

  - [x] 6.3 Write property test for retired IDs never reassigned (P2)
    - **Property 2: Retired Consumer IDs Are Never Reassigned**
    - Generate random interleaved create/delete sequences (1–500 ops)
    - Assert no deleted ID appears in subsequent assignments
    - Use jqwik with minimum 100 iterations
    - **Validates: Requirements 3.6**

  - [x] 6.4 Create `TestDataProducer` in `org.apache.kafka.server.record.mla.test` (under `src/test/`)
    - Wrap `KafkaProducer` and `ConsumerIdRegistryClient`
    - Implement `produce(topic, key, value, authorizedPrincipals)`:
      - Resolve principals to Consumer IDs via registry client
      - Build authorization bitmap using `AuthorizationBitmap.create()`
      - Attach `mla-authz-bitmap` header and send record
    - Implement `refreshConsumerIds()` and `close()`
    - _Requirements: 4.1, 5.1, 5.6_

  - [x] 6.5 Create `TestDataConsumer` in `org.apache.kafka.server.record.mla.test` (under `src/test/`)
    - Implement `subscribe(String topic)`, `poll(Duration)`, `getPrincipal()`
    - Collect received records for assertion in tests
    - _Requirements: 7.1, 8.1, 8.2_

- [x] 7. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Integration tests
  - [x] 8.1 Write integration tests for plugin loading and topic activation
    - Test broker starts with valid plugin config
    - Test broker fails to start with invalid plugin class
    - Test plugin invoked only for topics with matching `record.fetch.plugins` config
    - Test non-MLA topic delivers all records without plugin invocation
    - _Requirements: 1.2, 1.3, 2.3, 9.1, 9.2_

  - [x] 8.2 Write integration tests for consumer deletion and registry updates
    - Test: register consumer → delete consumer → consumer stops receiving new records
    - Test: MLAPlugin picks up new consumer registrations without broker restart
    - _Requirements: 3.5, 3.6, 6.1_

  - [x] 8.3 Write integration tests for offset advancement and watermark correctness
    - Test: after filtering, subsequent fetches start after filtered records
    - Test: high watermark and log start offset are correct in fetch responses
    - Test: consumer can commit offsets for records it did not receive
    - _Requirements: 11.1, 11.2, 11.3_

- [ ] 9. End-to-end tests
  - [x] 9.1 Write E2E test: Selective authorization with multiple consumers (E2E Scenario 1)
    - Register 5 consumers (c0–c4), create MLA-enabled topic
    - Produce 4 records with varying authorization sets (only-c0, c0-and-c2, all, none)
    - Assert each consumer receives exactly its authorized records
    - Run with header stripping enabled and disabled
    - _Requirements: 5.1, 5.6, 7.1, 7.2, 7.3, 7.4_

  - [x] 9.2 Write E2E test: Consumer registration after record production (E2E Scenario 2)
    - Register 3 consumers, produce record authorized for c0, c2, c5 (c5 not yet registered)
    - Register c5 after production, subscribe and seek to beginning
    - Assert c5 receives the record (bitmap already had bit 5 set)
    - _Requirements: 5.1, 7.2_

  - [x] 9.3 Write E2E test: Consumer deletion stops delivery (E2E Scenario 3)
    - Register 3 consumers, produce record for c0 and c1
    - Delete c1, produce another record for c0 and c1
    - Assert c1 does not receive the second record
    - _Requirements: 3.5, 3.6, 7.2_

  - [x] 9.4 Write E2E test: Non-MLA topic passthrough (E2E Scenario 4)
    - Create a regular topic (no `record.fetch.plugins`)
    - Produce records without authorization headers
    - Assert all consumers receive all records
    - _Requirements: 9.1, 9.2_

- [x] 10. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP.
- Each task references specific requirements for traceability.
- Property-based tests use jqwik with a minimum of 100 iterations per property.
- Checkpoints ensure incremental validation at key milestones.
- Test-only components (TestAdminClient, TestDataProducer, TestDataConsumer) live under `src/test/` and are not included in production builds.
- Documentation tasks (Requirements 12, 13) are excluded because they are not coding tasks.
