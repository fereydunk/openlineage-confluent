#!/usr/bin/env python3
"""Producer / Consumer demo for testing Confluent lineage detection.

This script produces messages to a demo topic and then consumes them
using a named consumer group. The consumer group will appear in the
lineage bridge via the Metrics API consumer_lag_offsets signal.

Requirements:
    pip install confluent-kafka

Credentials needed (cluster-scoped Kafka API key, NOT the cloud API key):
    Create under: Confluent Cloud → Cluster → API Keys → Add key → Kafka cluster scoped

Usage:
    # Produce 20 messages, then consume them
    python scripts/producer_consumer_demo.py

    # Just produce
    python scripts/producer_consumer_demo.py --mode produce

    # Just consume (run alongside a producer in another terminal)
    python scripts/producer_consumer_demo.py --mode consume

    # Override topic or group
    python scripts/producer_consumer_demo.py --topic my-topic --group my-app-group

After running, wait ~2-3 minutes for the Metrics API to ingest the data, then:
    ol-confluent validate --config config.yaml
The consumer group "ol-lineage-demo-consumer" should appear as a lineage node.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime

# ── Credentials ───────────────────────────────────────────────────────────────
# Set these as environment variables or edit the defaults below.
# Use a Kafka cluster-scoped API key (NOT the cloud API key).

BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP",  "pkc-pgq85.us-west-2.aws.confluent.cloud:9092")
KAFKA_API_KEY    = os.getenv("KAFKA_API_KEY",    "")   # set via env: export KAFKA_API_KEY=...
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET", "")  # set via env: export KAFKA_API_SECRET=...

DEFAULT_TOPIC  = "ol-lineage-demo"
DEFAULT_GROUP  = "ol-lineage-demo-consumer"
NUM_MESSAGES   = 20


def _kafka_config() -> dict:
    if not KAFKA_API_KEY or not KAFKA_API_SECRET:
        print(
            "ERROR: Set KAFKA_API_KEY and KAFKA_API_SECRET environment variables.\n"
            "Create a Kafka cluster-scoped key under:\n"
            "  Confluent Cloud → Cluster → API Keys → Add key → Kafka cluster scoped",
            file=sys.stderr,
        )
        sys.exit(1)
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     KAFKA_API_KEY,
        "sasl.password":     KAFKA_API_SECRET,
    }


def produce(topic: str, n: int) -> None:
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)

    cfg = _kafka_config()
    cfg["client.id"] = "ol-lineage-demo-producer"

    p = Producer(cfg)
    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if err:
            print(f"  Delivery failed: {err}", file=sys.stderr)
        else:
            delivered += 1

    print(f"Producing {n} messages to topic '{topic}' ...")
    for i in range(n):
        payload = json.dumps({
            "id":        str(uuid.uuid4()),
            "sequence":  i,
            "amount":    round(100.0 + i * 13.7, 2),
            "timestamp": datetime.now(UTC).isoformat(),
        })
        p.produce(topic, value=payload.encode(), on_delivery=on_delivery)
        if i % 5 == 0:
            p.poll(0)

    p.flush(timeout=30)
    print(f"  Produced {delivered}/{n} messages OK.")


def consume(topic: str, group: str, timeout_s: int = 30) -> None:
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)

    cfg = _kafka_config()
    cfg.update({
        "group.id":           group,
        "client.id":          f"{group}-instance-1",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })

    c = Consumer(cfg)
    c.subscribe([topic])

    print(f"Consuming from '{topic}' as group '{group}' (timeout {timeout_s}s) ...")
    print(f"  Consumer group '{group}' will appear in lineage graph after ~2-3 min.")

    received = 0
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"  Reached end of partition {msg.partition()}")
                else:
                    print(f"  Error: {msg.error()}", file=sys.stderr)
                continue
            received += 1
            if received <= 3 or received % 5 == 0:
                data = json.loads(msg.value().decode())
                print(f"  [{received}] partition={msg.partition()} offset={msg.offset()} "
                      f"id={data.get('id', '?')[:8]}... amount={data.get('amount')}")
    finally:
        c.close()

    print(f"  Consumed {received} messages total.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode",  choices=["produce", "consume", "both"], default="both")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group", default=DEFAULT_GROUP)
    parser.add_argument("--count", type=int, default=NUM_MESSAGES)
    args = parser.parse_args()

    print(f"Bootstrap: {BOOTSTRAP}")
    print(f"Topic:     {args.topic}")
    print(f"Group:     {args.group}")
    print()

    if args.mode in ("produce", "both"):
        produce(args.topic, args.count)
        print()

    if args.mode in ("consume", "both"):
        consume(args.topic, args.group)
        print()

    print("Done.")
    print()
    print("Next steps:")
    print("  1. Wait ~2-3 minutes for Metrics API ingestion lag")
    print("  2. Run: ol-confluent validate --config config.yaml")
    print(f"  3. Look for consumer group '{args.group}' in the lineage edges table")


if __name__ == "__main__":
    main()
