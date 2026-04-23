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

producer.flush()
