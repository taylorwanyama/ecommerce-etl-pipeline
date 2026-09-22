"""
generate_events.py

Simulates the "upstream system" — in a real company this script would not
exist, because these events would already be arriving from your website's
backend, a mobile app SDK, or a message queue like Kafka/Kinesis. Since this
project is meant to be self-contained, this script plays that role: it
writes raw, slightly-messy event data to disk so the rest of the pipeline
has something realistic to process.

WHY THE DATA IS DELIBERATELY MESSY:
Real event data is never perfectly clean. Clocks drift, mobile clients retry
and send duplicates, a frontend bug ships a bad price, a field is renamed
without warning. If this generator produced perfect data, the validation
step in ingest.py would never have anything to catch, and you'd learn
nothing about why that step exists. So this script intentionally injects a
small, configurable percentage of bad rows.

WHY EVENTS ARE PARTITIONED BY DATE ON DISK:
Notice the output path: data/bronze/dt=YYYY-MM-DD/events.jsonl
This "dt=" folder naming is called Hive-style partitioning, and it's an
industry-standard convention (used by Spark, Hive, AWS Athena, Presto, and
most data lakes). It matters for two real reasons:
  1. You can reprocess or backfill a single day without touching any other
     day's data.
  2. Tools that read partitioned data can skip folders that don't match a
     query's date filter ("partition pruning"), which is a huge performance
     win at real-world data volumes.

USAGE:
    python generate_events.py --date 2026-09-22 --num-events 5000
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

PRODUCTS = [
    ("P100", "electronics", 299.99),
    ("P101", "electronics", 89.50),
    ("P102", "home", 24.99),
    ("P103", "home", 149.00),
    ("P104", "apparel", 39.99),
    ("P105", "apparel", 19.99),
    ("P106", "beauty", 14.50),
    ("P107", "beauty", 32.00),
    ("P108", "sports", 59.99),
    ("P109", "sports", 199.99),
]


def random_timestamp(date_str: str) -> str:
    """Random timestamp somewhere within the given calendar day."""
    base = datetime.fromisoformat(date_str)
    offset_seconds = random.randint(0, 86399)
    return (base + timedelta(seconds=offset_seconds)).isoformat()


def make_good_event(date_str: str) -> dict:
    """Build one plausible, valid event following a page_view -> add_to_cart
    -> purchase funnel shape (each step less likely than the last, like a
    real conversion funnel)."""
    product_id, category, price = random.choice(PRODUCTS)
    roll = random.random()
    if roll < 0.60:
        event_type = "page_view"
    elif roll < 0.85:
        event_type = "add_to_cart"
    else:
        event_type = "purchase"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": random_timestamp(date_str),
        "user_id": f"u{random.randint(1, 2000)}",
        "session_id": f"s{random.randint(1, 5000)}",
        "product_id": product_id,
        "product_category": category,
    }

    if event_type in ("add_to_cart", "purchase"):
        event["quantity"] = random.randint(1, 3)
    if event_type == "purchase":
        event["unit_price"] = price

    return event


def corrupt_event(event: dict) -> dict:
    """Take an otherwise-good event and break it in one of several ways
    that mirror real-world data quality issues."""
    corruption = random.choice(
        ["missing_field", "bad_timestamp", "negative_price", "unknown_type", "duplicate_id_placeholder"]
    )
    event = dict(event)  # don't mutate the original

    if corruption == "missing_field":
        # Simulates a client-side bug that forgot to set a field.
        field = random.choice(["user_id", "session_id", "product_id"])
        event[field] = ""
    elif corruption == "bad_timestamp":
        # Simulates a clock bug or a malformed date string from a client.
        event["timestamp"] = "not-a-real-timestamp"
    elif corruption == "negative_price":
        event["event_type"] = "purchase"
        event["unit_price"] = -19.99
        event["quantity"] = 1
    elif corruption == "unknown_type":
        # Simulates a new event type a mobile team shipped without telling you.
        event["event_type"] = "wishlist_add"
    # "duplicate_id_placeholder" is handled by the caller (it just reuses
    # an existing event_id rather than modifying this one).

    return event


def generate(date_str: str, num_events: int, bad_rate: float, out_dir: str) -> str:
    partition_dir = os.path.join(out_dir, f"dt={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    out_path = os.path.join(partition_dir, "events.jsonl")

    events = []
    seen_ids = []

    for _ in range(num_events):
        event = make_good_event(date_str)

        if random.random() < bad_rate:
            if seen_ids and random.random() < 0.2:
                # Simulate a duplicate delivery: reuse a previous event_id.
                # This happens for real — most event pipelines only guarantee
                # "at-least-once" delivery, so consumers must expect and
                # handle duplicates rather than assume every event is unique.
                event["event_id"] = random.choice(seen_ids)
            else:
                event = corrupt_event(event)

        events.append(event)
        seen_ids.append(event.get("event_id", ""))

    # Overwrite the file each run for this partition — see README for why
    # "idempotent by overwrite" matters. This generator isn't part of the
    # pipeline itself, but it's good practice to build that habit everywhere.
    with open(out_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce events")
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--num-events", type=int, default=5000)
    parser.add_argument("--bad-rate", type=float, default=0.03, help="Fraction of events that are intentionally messy")
    parser.add_argument("--out-dir", type=str, default="data/bronze")
    args = parser.parse_args()

    path = generate(args.date, args.num_events, args.bad_rate, args.out_dir)
    print(f"Wrote {args.num_events} events ({args.bad_rate:.0%} intentionally bad) to {path}")


if __name__ == "__main__":
    main()
