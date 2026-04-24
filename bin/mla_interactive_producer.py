#!/usr/bin/env python3
"""
Interactive MLA producer.

Reads the Consumer ID Registry, displays registered users, then enters
an interactive loop where you type a message and select which consumers
are authorized to receive it.

Usage:
    python3 bin/mla_interactive_producer.py [--bootstrap localhost:9092]
                                            [--topic mla-demo]
                                            [--registry _consumer_id_registry]
"""

import struct
import sys
import argparse
from confluent_kafka import Producer, Consumer, TopicPartition


def read_registry(bootstrap, registry_topic):
    """Read all consumer ID mappings from the registry topic."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"mla-interactive-{id(c) if False else 'reader'}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })

    # Get partition count
    metadata = c.list_topics(registry_topic, timeout=10)
    topic_meta = metadata.topics.get(registry_topic)
    if not topic_meta or topic_meta.error is not None:
        print(f"Registry topic '{registry_topic}' not found.")
        c.close()
        return {}

    partitions = [TopicPartition(registry_topic, p, 0) for p in topic_meta.partitions]
    c.assign(partitions)

    mappings = {}
    empty_polls = 0
    while empty_polls < 3:
        msg = c.poll(2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0
        key = msg.key().decode("utf-8") if msg.key() else None
        if key is None:
            continue
        if msg.value() is None:
            # Tombstone — consumer deleted
            mappings.pop(key, None)
        elif len(msg.value()) == 4:
            consumer_id = struct.unpack(">i", msg.value())[0]
            mappings[key] = consumer_id

    c.close()
    return mappings


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


def format_bitmap(bitmap_bytes):
    """Format bitmap as hex and bit string."""
    if not bitmap_bytes:
        return "(empty)", "(empty)"
    hex_str = bitmap_bytes.hex()
    bit_str = ' '.join(f'{b:08b}' for b in bitmap_bytes)
    return hex_str, bit_str


def print_registry(mappings):
    """Print the registered consumers in a table."""
    if not mappings:
        print("\n  No consumers registered in the registry.")
        return

    print("\n  Registered consumers:")
    print("  " + "-" * 40)
    print(f"  {'Principal':<25} {'Consumer ID':>10}")
    print("  " + "-" * 40)
    # Sort by consumer ID
    for principal, cid in sorted(mappings.items(), key=lambda x: x[1]):
        print(f"  {principal:<25} {cid:>10}")
    print("  " + "-" * 40)
    print()


def print_help():
    """Print interactive commands."""
    print()
    print("  Commands:")
    print("    <message text>          — then you'll be asked for authorized consumers and count")
    print("    list                    — show registered consumers again")
    print("    reload                  — re-read the registry topic")
    print("    help                    — show this help")
    print("    quit / exit             — exit the producer")
    print()


def interactive_loop(producer, topic, mappings, bootstrap, registry_topic):
    """Main interactive loop."""
    # Build reverse map: consumer_id -> principal
    id_to_principal = {v: k for k, v in mappings.items()}
    msg_counter = 0

    print_help()

    while True:
        try:
            text = input("  Message text [Hi]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye.")
            break

        if text.lower() in ("quit", "exit", "q"):
            print("  Bye.")
            break

        if text.lower() == "help":
            print_help()
            continue

        if text.lower() == "list":
            print_registry(mappings)
            continue

        if text.lower() == "reload":
            mappings = read_registry(bootstrap, registry_topic)
            id_to_principal = {v: k for k, v in mappings.items()}
            print_registry(mappings)
            continue

        if not text:
            text = "Hi"

        # Ask for authorized consumers
        print()
        print("  Who should receive this message?")
        print("  Enter consumer IDs separated by commas, or:")
        print("    all   — authorize all registered consumers")
        print("    none  — authorize nobody (empty bitmap)")
        print()

        # Show quick reference
        for cid in sorted(id_to_principal.keys()):
            print(f"    {cid} = {id_to_principal[cid]}")
        print()

        try:
            selection = input("  Authorized consumer IDs: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye.")
            break

        if selection.lower() == "all":
            selected_ids = list(mappings.values())
        elif selection.lower() == "none" or selection == "":
            selected_ids = []
        else:
            try:
                selected_ids = [int(x.strip()) for x in selection.split(",")]
            except ValueError:
                print("  Invalid input. Enter comma-separated numbers, 'all', or 'none'.")
                continue

        # Build bitmap
        bitmap = make_bitmap(selected_ids)
        hex_str, bit_str = format_bitmap(bitmap)

        # Show what we're about to send
        authorized_names = [id_to_principal.get(cid, f"ID-{cid}") for cid in sorted(selected_ids)]
        print()
        print(f"  Bitmap hex:  {hex_str}")
        print(f"  Bitmap bits: {bit_str}")
        print(f"  Authorized:  {', '.join(authorized_names) if authorized_names else '(nobody)'}")

        # Ask how many copies to send
        try:
            count_str = input("  How many messages to send? [1]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye.")
            break

        count = 1
        if count_str:
            try:
                count = int(count_str)
                if count < 1:
                    count = 1
            except ValueError:
                print("  Invalid number, sending 1.")
                count = 1

        # Produce
        headers = {"mla-authz-bitmap": bitmap} if bitmap else {}
        auth_suffix = ", ".join(authorized_names) if authorized_names else "nobody"

        for n in range(count):
            msg_counter += 1
            key = f"msg-{msg_counter}"
            if count == 1:
                value = f"{text} [{auth_suffix}]"
            else:
                value = f"{text} [{n+1}/{count}] [{auth_suffix}]"

            producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                headers=headers,
            )

        producer.flush()

        if count == 1:
            print(f"  Sent 'msg-{msg_counter}': \"{value}\"")
        else:
            print(f"  Sent {count} messages (msg-{msg_counter - count + 1} to msg-{msg_counter}): \"{text} [1..{count}] [{auth_suffix}]\"")
        print()


def main():
    parser = argparse.ArgumentParser(description="Interactive MLA producer")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="mla-demo",
                        help="Target topic (default: mla-demo)")
    parser.add_argument("--registry", default="_consumer_id_registry",
                        help="Consumer ID Registry topic (default: _consumer_id_registry)")
    args = parser.parse_args()

    print()
    print("=" * 50)
    print("  MLA Interactive Producer")
    print(f"  Bootstrap: {args.bootstrap}")
    print(f"  Topic:     {args.topic}")
    print(f"  Registry:  {args.registry}")
    print("=" * 50)

    # Read registry
    print("\n  Reading consumer registry...")
    mappings = read_registry(args.bootstrap, args.registry)
    print_registry(mappings)

    if not mappings:
        print("  Warning: No consumers registered. Messages will be produced")
        print("  but nobody will be authorized to receive them.")
        print()

    # Create producer
    producer = Producer({"bootstrap.servers": args.bootstrap})

    try:
        interactive_loop(producer, args.topic, mappings, args.bootstrap, args.registry)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
