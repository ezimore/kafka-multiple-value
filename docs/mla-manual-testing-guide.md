# Message-Level Authorization (MLA) — Manual Testing Guide

This guide walks through setting up and manually testing the MLA feature end-to-end on a local Kafka cluster.

## Overview

MLA adds per-record authorization to Kafka. The flow is:

1. An administrator creates a **Consumer ID Registry** topic and registers consumers with sequential integer IDs.
2. A **Data Producer** reads consumer IDs, builds an authorization bitmap for each record, and attaches it as a record header.
3. The **MLAPlugin** on the broker filters records at fetch time — each consumer only receives records whose bitmap authorizes them.

## Prerequisites

- Java 17+
- This Kafka source tree built: `./gradlew jar -x test`
- A terminal with multiple tabs/panes (or tmux)

## 1. Build Kafka

### clean the previous builds and rebuild everything
```bash 
./gradlew clean jar -x test -x spotbugsMain -x checkstyleMain
```

### rebuild only the modules which were changed
```bash 
./gradlew jar -x test -x spotbugsMain -x checkstyleMain
```

## 2. Generate a Cluster ID and Format Storage

```bash
export KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"

./bin/kafka-storage.sh format \
  --config config/server.properties \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --standalone
```

## 3. Configure the Broker with the MLAPlugin

Edit `config/server.properties` and add these lines at the end:

```properties
# Load the MLA plugin at broker startup
record.fetch.plugin.classes=org.apache.kafka.server.record.mla.MLAPlugin

# MLA plugin configuration
mla.strip.authorization.header=true
mla.consumer.id.registry.topic=_consumer_id_registry
```

The `mla.strip.authorization.header` setting controls whether the `mla-authz-bitmap` header is removed from records before delivery to consumers. Set to `false` if you want consumers to see the raw bitmap.

## 4. Start the Broker

```bash
./bin/kafka-server-start.sh config/server.properties
```

Wait until you see `Kafka Server started` in the logs.

## 5. Create the Consumer ID Registry Topic

The registry is a standard compacted topic. Create it with 1 partition:

```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create \
  --topic _consumer_id_registry \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact
```

### remove the topic if needed
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic _consumer_id_registry
```

## 6. Register Consumers

Each consumer needs a unique, sequential integer ID starting from 0. The registry record format is:

- **Key**: the Kafka principal string (e.g., `User:alice`)
- **Value**: 4-byte big-endian integer (the consumer ID)

Use the console producer with key separator to write registry entries. Since the value must be raw bytes (4-byte big-endian int), the simplest approach is a small Java or Python helper.

```bash
python3 ./bin/mla_register_consumers.py
```

### Option A: Using a Python helper script

install confluent-kafka

```bash
sudo apt install python3.12-venv
python3 -m venv ~/kafka-venv
source ~/kafka-venv/bin/activate
pip install confluent-kafka
```

Create `mla_register_consumers.py`:

```python
#!/usr/bin/env python3
"""Register consumers in the Consumer ID Registry topic."""

import struct
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
REGISTRY_TOPIC = "_consumer_id_registry"

producer = Producer({"bootstrap.servers": BOOTSTRAP})

consumers = [
    ("User:alice", 0),
    ("User:bob",   1),
    ("User:carol", 2),
    ("User:dave",  3),
    ("User:eve",   4),
]

for principal, consumer_id in consumers:
    key = principal.encode("utf-8")
    value = struct.pack(">i", consumer_id)  # 4-byte big-endian int
    producer.produce(REGISTRY_TOPIC, key=key, value=value)
    producer.flush()
    print(f"Registered {principal} -> consumer ID {consumer_id}")

producer.flush()```

Run it:

```bash
source ~/kafka-venv/bin/activate
python3 mla_register_consumers.py
```




### Option B: Using a Java one-liner with jshell

```bash
# From the Kafka source root, after building:
jshell --class-path "$(find clients/build/libs core/build/libs -name '*.jar' | tr '\n' ':')" << 'EOF'
import org.apache.kafka.clients.producer.*;
import java.nio.ByteBuffer;
import java.util.*;

var props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

var producer = new KafkaProducer<String, byte[]>(props);
var consumers = Map.of("User:alice", 0, "User:bob", 1, "User:carol", 2, "User:dave", 3, "User:eve", 4);

for (var entry : consumers.entrySet()) {
    byte[] value = ByteBuffer.allocate(4).putInt(entry.getValue()).array();
    producer.send(new ProducerRecord<>("_consumer_id_registry", entry.getKey(), value)).get();
    System.out.println("Registered " + entry.getKey() + " -> ID " + entry.getValue());
}
producer.close();
/exit
EOF
```

### Verify registrations

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic _consumer_id_registry \
  --from-beginning \
  --property print.key=true \
  --max-messages 5
```

You should see 5 records with the principal names as keys.

## 7. Create an MLA-Enabled Topic

```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create \
  --topic mla-demo \
  --partitions 1 \
  --replication-factor 1 \
  --config record.fetch.plugins=org.apache.kafka.server.record.mla.MLAPlugin
```

The `record.fetch.plugins` topic-level config tells the broker which plugins are active for this topic.

## 8. Produce Records with Authorization Bitmaps

Each record needs an `mla-authz-bitmap` header containing the authorization bitmap. The bitmap uses big-endian bit order:

- Bit 0 = MSB of byte 0 (mask `0x80`)
- Bit N is in byte `N/8` at mask `0x80 >>> (N % 8)`

### Bitmap examples

| Authorized consumers | Consumer IDs | Bitmap bytes (hex) |
|---|---|---|
| alice only | {0} | `80` |
| alice and carol | {0, 2} | `A0` |
| bob and dave | {1, 3} | `50` |
| all five | {0,1,2,3,4} | `F8` |
| none | {} | (empty) |

### Produce with the Python helper

Create `mla_produce_test_records.py`:

```python
#!/usr/bin/env python3
"""Produce records with MLA authorization bitmaps."""

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "mla-demo"
HEADER_KEY = "mla-authz-bitmap"

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def make_bitmap(consumer_ids):
    """Build a big-endian authorization bitmap for the given consumer IDs."""
    if not consumer_ids:
        return b""
    max_id = max(consumer_ids)
    num_bytes = (max_id // 8) + 1
    bitmap = bytearray(num_bytes)
    for cid in consumer_ids:
        byte_idx = cid // 8
        mask = 0x80 >> (cid % 8)
        bitmap[byte_idx] |= mask
    return bytes(bitmap)


def send(key, value, bitmap, description):
    headers = {HEADER_KEY: bitmap} if bitmap else {}
    producer.produce(TOPIC, key=key, value=value, headers=headers)
    producer.flush()
    if bitmap:
        bits = ''.join(f'{b:08b}' for b in bitmap)
        print(f"Sent {key.decode()}: {description}, bitmap={bitmap.hex()} bits={bits}")
    else:
        print(f"Sent {key.decode()}: {description}, bitmap=(empty)")


# Record 1: only alice (ID 0)
send(b"msg-1", b"secret-for-alice", make_bitmap([0]),
     "authorized for alice only")

# Record 2: alice (0) and carol (2)
send(b"msg-2", b"shared-alice-carol", make_bitmap([0, 2]),
     "authorized for alice+carol")

# Record 3: bob (1) and dave (3)
send(b"msg-3", b"shared-bob-dave", make_bitmap([1, 3]),
     "authorized for bob+dave")

# Record 4: everyone (0,1,2,3,4)
send(b"msg-4", b"broadcast-to-all", make_bitmap([0, 1, 2, 3, 4]),
     "authorized for all")

# Record 5: nobody (empty bitmap)
send(b"msg-5", b"authorized-for-nobody", make_bitmap([]),
     "authorized for nobody")

print("\nAll records produced.")
```

Run it:

```bash
python3 bin/mla_produce_test_records.py
```

## 9. Consume as Different Users

The MLAPlugin identifies consumers by their Kafka principal (e.g., `User:alice`). In a **PLAINTEXT** (no security) cluster, all consumers authenticate as `User:ANONYMOUS`. To test with different principals, you need to enable SASL authentication.

To test with multiple distinct principals, configure SASL/PLAIN on the broker.

### 1. Update broker config

Add to `config/server.properties`:

```properties
# SASL/PLAIN listener
listeners=PLAINTEXT://:9092,CONTROLLER://:9093,SASL_PLAINTEXT://:9094
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093,SASL_PLAINTEXT://localhost:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="admin" \
    password="admin-secret" \
    user_admin="admin-secret" \
    user_alice="alice-secret" \
    user_bob="bob-secret" \
    user_carol="carol-secret" \
    user_dave="dave-secret" \
    user_eve="eve-secret";
```

### 2. Start the broker with JAAS

```bash
./bin/kafka-server-start.sh config/server.properties
```

### 3. Create client JAAS configs

For each user, create a properties file. Example for alice — `config/client-alice.properties`:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="alice" \
    password="alice-secret";
```

Create similar files for bob, carol, dave, eve.

### 4. Register consumers with SASL principals

When using SASL/PLAIN, the principal format is `User:alice` (not `User:ANONYMOUS`). The registrations in step 6 already use this format.

#### 6. Consume as each user

Open separate terminals for each consumer:

**Terminal — alice (consumer ID 0):**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic mla-demo \
  --from-beginning \
  --consumer.config config/client-alice.properties
```

**Terminal — bob (consumer ID 1):**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic mla-demo \
  --from-beginning \
  --consumer.config config/client-bob.properties
```

**Terminal — carol (consumer ID 2):**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic mla-demo \
  --from-beginning \
  --consumer.config config/client-carol.properties
```

**Terminal — dave (consumer ID 3):**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic mla-demo \
  --from-beginning \
  --consumer.config config/client-dave.properties
```

**Terminal — eve (consumer ID 4):**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic mla-demo \
  --from-beginning \
  --consumer.config config/client-eve.properties
```

## 10. Expected Results

Given the 5 records produced in step 8:

| Record | Value | alice (0) | bob (1) | carol (2) | dave (3) | eve (4) |
|---|---|---|---|---|---|---|
| msg-1 | secret-for-alice | ✅ | ❌ | ❌ | ❌ | ❌ |
| msg-2 | shared-alice-carol | ✅ | ❌ | ✅ | ❌ | ❌ |
| msg-3 | shared-bob-dave | ❌ | ✅ | ❌ | ✅ | ❌ |
| msg-4 | broadcast-to-all | ✅ | ✅ | ✅ | ✅ | ✅ |
| msg-5 | authorized-for-nobody | ❌ | ❌ | ❌ | ❌ | ❌ |

- **alice** sees: msg-1, msg-2, msg-4 (3 records)
- **bob** sees: msg-3, msg-4 (2 records)
- **carol** sees: msg-2, msg-4 (2 records)
- **dave** sees: msg-3, msg-4 (2 records)
- **eve** sees: msg-4 (1 record)
- **msg-5** is delivered to nobody (empty bitmap)

## 11. Testing Consumer Deletion

To verify that deleting a consumer stops delivery:

### Delete bob from the registry

Write a tombstone (null value) for `User:bob`:

```python
#!/usr/bin/env python3
"""Delete a consumer from the registry by writing a tombstone."""
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
producer.send("_consumer_id_registry", key=b"User:bob", value=None).get()
print("Deleted User:bob (tombstone written)")
producer.close()
```

After a few seconds, the MLAPlugin picks up the tombstone. Now produce a new record authorized for bob:

```python
producer.send("mla-demo", key=b"msg-6", value=b"after-bob-deleted",
              headers=[("mla-authz-bitmap", make_bitmap([1, 3]))]).get()
```

**Expected**: bob does NOT receive msg-6, even though bit 1 is set in the bitmap. dave still receives it.

## 12. Testing Non-MLA Topics

Create a regular topic without the `record.fetch.plugins` config:

```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create \
  --topic regular-topic \
  --partitions 1 \
  --replication-factor 1
```

Produce and consume normally — all consumers receive all records, no filtering occurs.

## 13. Configuration Reference

### Broker-level

| Property | Default | Description |
|---|---|---|
| `record.fetch.plugin.classes` | (empty) | Comma-separated list of RecordFetchPlugin class names to load at startup |
| `mla.strip.authorization.header` | `true` | Remove the `mla-authz-bitmap` header from delivered records |
| `mla.consumer.id.registry.topic` | `_consumer_id_registry` | Name of the Consumer ID Registry compacted topic |

### Topic-level

| Property | Default | Description |
|---|---|---|
| `record.fetch.plugins` | (empty) | Comma-separated list of RecordFetchPlugin class names active for this topic |

### Authorization Bitmap Format

- Big-endian bit order: bit 0 = MSB of byte 0
- Bit N is in byte `N/8` at mask `0x80 >>> (N % 8)`
- Variable-length: only as many bytes as needed for the highest consumer ID
- Empty bitmap = unauthorized for all consumers

### Consumer ID Registry Record Format

- **Topic**: compacted topic (default `_consumer_id_registry`)
- **Key**: UTF-8 string — the Kafka principal (e.g., `User:alice`)
- **Value**: 4-byte big-endian integer — the consumer ID, or `null` for tombstone (deletion)

## Troubleshooting

- **No records delivered**: Check that the consumer's principal matches a registered entry in the registry topic. In PLAINTEXT mode, the principal is `User:ANONYMOUS`.
- **All records delivered (no filtering)**: Verify the topic has `record.fetch.plugins` set. Check with:
  ```bash
  ./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name mla-demo --describe
  ```
- **Plugin not loading**: Check broker logs for `MLAPlugin configured` and `MLAPlugin started` messages. Verify `record.fetch.plugin.classes` is set in broker config.
- **Registry not updating**: The MLAPlugin's internal consumer polls every 500ms. Allow a few seconds after writing to the registry topic.
