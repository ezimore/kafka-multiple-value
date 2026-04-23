#!/usr/bin/env python3
"""
MLA-aware consumer that shows offsets, including gaps from filtered records.

When a message is received, prints the offset and message.
When offsets are skipped (filtered by MLA), prints the gap.

Usage:
    python3 bin/mla_consumer.py [--bootstrap localhost:9092] [--topic mla-demo]
                                [--sasl-bootstrap localhost:9094]
                                [--user alice] [--password alice-secret]
"""

import argparse
import time
from confluent_kafka import Consumer, TopicPartition


def main():
    parser = argparse.ArgumentParser(description="MLA-aware consumer with offset tracking")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="Kafka bootstrap servers for PLAINTEXT (default: localhost:9092)")
    parser.add_argument("--sasl-bootstrap",
                        help="Kafka bootstrap servers for SASL (e.g., localhost:9094)")
    parser.add_argument("--topic", default="mla-demo",
                        help="Topic to consume (default: mla-demo)")
    parser.add_argument("--user", help="SASL username (enables SASL auth)")
    parser.add_argument("--password", help="SASL password")
    args = parser.parse_args()

    # Build consumer config
    config = {
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "fetch.wait.max.ms": "500",
    }

    if args.user and args.password:
        bootstrap = args.sasl_bootstrap or args.bootstrap
        config["bootstrap.servers"] = bootstrap
        config["security.protocol"] = "SASL_PLAINTEXT"
        config["sasl.mechanism"] = "PLAIN"
        config["sasl.username"] = args.user
        config["sasl.password"] = args.password
        config["group.id"] = f"mla-consumer-{args.user}-{int(time.time())}"
        principal = f"User:{args.user}"
    else:
        config["bootstrap.servers"] = args.bootstrap
        config["group.id"] = f"mla-consumer-anon-{int(time.time())}"
        principal = "User:ANONYMOUS"

    print()
    print(f"  Consumer: {principal}")
    print(f"  Topic:    {args.topic}")
    print(f"  Bootstrap: {config['bootstrap.servers']}")
    print()

    c = Consumer(config)

    # Get partition info and assign manually at offset 0
    metadata = c.list_topics(args.topic, timeout=10)
    topic_meta = metadata.topics.get(args.topic)
    if not topic_meta or topic_meta.error is not None:
        print(f"  Topic '{args.topic}' not found.")
        c.close()
        return

    partitions = [TopicPartition(args.topic, p, 0) for p in topic_meta.partitions]
    c.assign(partitions)

    # Track last known position per partition
    last_position = {p: 0 for p in topic_meta.partitions}

    print("  Waiting for messages (Ctrl+C to stop)...")
    print("  " + "-" * 60)

    idle_count = 0
    try:
        while True:
            msg = c.poll(1.0)

            # After every poll, check position for all partitions to detect
            # offset advancement from filtered (empty-batch) records
            check_position_advancement(c, partitions, last_position)

            if msg is None:
                idle_count += 1
                if idle_count >= 30:
                    print("  (no new messages for 30 seconds)")
                    idle_count = 0
                continue

            if msg.error():
                print(f"  Error: {msg.error()}")
                continue

            idle_count = 0
            partition = msg.partition()
            offset = msg.offset()

            # Check for gap before this message (filtered records)
            expected = last_position.get(partition, 0)
            if offset > expected:
                skipped = offset - expected
                print(f"  [P{partition}] offset {expected}..{offset-1}"
                      f" — {skipped} record(s) filtered (not authorized)")

            # Print the received message
            key = msg.key().decode("utf-8") if msg.key() else "(null)"
            value = msg.value().decode("utf-8") if msg.value() else "(null)"

            headers = dict(msg.headers()) if msg.headers() else {}
            has_bitmap = "mla-authz-bitmap" in headers

            # Skip advancement records injected by the broker for offset tracking
            # if "record-fetch-plugin-filtered" in headers:
            #    last_position[partition] = offset + 1
            #    continue

            print(f"  [P{partition}] offset {offset}: key={key}  value=\"{value}\""
                  f"{'  [has bitmap header]' if has_bitmap else ''}")

            # Update position to next expected offset
            last_position[partition] = offset + 1

    except KeyboardInterrupt:
        print("\n  Stopped.")
    finally:
        c.close()


def check_position_advancement(consumer, partitions, last_position):
    """Check if the consumer position advanced past filtered records (empty batches)."""
    for tp in partitions:
        try:
            current_pos = consumer.position([tp])[0].offset
        except Exception:
            continue

        if current_pos < 0:
            continue

        expected = last_position.get(tp.partition, 0)
        if current_pos > expected:
            skipped = current_pos - expected
            print(f"  [P{tp.partition}] offset {expected}..{current_pos-1}"
                  f" — {skipped} record(s) filtered (not authorized)")
            last_position[tp.partition] = current_pos


if __name__ == "__main__":
    main()
