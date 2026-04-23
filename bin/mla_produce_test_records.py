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

