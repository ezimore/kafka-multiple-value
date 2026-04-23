#!/usr/bin/env python3
"""Read and display the Consumer ID Registry topic."""

import struct
from confluent_kafka import Consumer

def on_assign(consumer, partitions):
    for p in partitions:
        p.offset = 0  # OFFSET_BEGINNING
    consumer.assign(partitions)

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "registry-reader",
    "auto.offset.reset": "earliest",
})
c.subscribe(["_consumer_id_registry"], on_assign=on_assign)

count = 0
while count < 5:
    msg = c.poll(5.0)
    if msg is None:
        print(f"msg is None")
        break
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    key = msg.key().decode("utf-8")
    if msg.value():
        raw = msg.value()
        numeric = struct.unpack(">i", raw)[0]
        hex_str = raw.hex()
        #print(f"{key} -> id={numeric}  bytes={raw}  hex=0x{hex_str}")
        print(f"{key} -> id={numeric} hex=0x{hex_str}")
    else:
        print(f"{key} -> TOMBSTONE")
    count += 1

c.close()
