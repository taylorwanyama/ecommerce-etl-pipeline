"""
pipeline/transform.py

Stage 2 of the pipeline: silver (validated, row-level events) -> gold
(aggregated business metrics).

WHAT THIS STAGE IS RESPONSIBLE FOR:
Turning clean but low-level event rows into the numbers a business
stakeholder would actually ask for:
  - Revenue per product category, per day.
  - Top-selling products, per day.
  - A conversion funnel: how many page_views turned into add_to_cart, and
    how many of those turned into purchase.

WHY THIS LIVES SEPARATELY FROM INGESTION:
By the time data reaches this stage, we already trust it (that was
ingest.py's job). This stage is allowed to assume clean input and focus
entirely on business logic. That separation is what lets you change a
revenue formula next quarter without touching — or re-testing — your data
quality checks at all, and vice versa. In a larger real-world system, this
is roughly the difference between a "staging" layer and a "marts" layer in
a dbt project, or a "silver" vs "gold" layer in a lakehouse.

WHY THIS STAGE IS ALSO IDEMPOTENT:
Just like ingestion, this function overwrites the gold output for a given
date rather than appending or incrementing counters. If you re-run
yesterday's batch because you fixed a bug, you get today's correct numbers,
not double-counted ones. Idempotency at every stage is what makes a batch
pipeline safe to retry — and retries happen constantly in production,
whether from transient failures, backfills, or bug fixes.
"""

import json
import os
from collections import defaultdict


def transform_date(date_str: str, silver_dir: str = "data/silver", gold_dir: str = "data/gold") -> dict:
    silver_path = os.path.join(silver_dir, f"dt={date_str}", "events.jsonl")
    gold_path_dir = os.path.join(gold_dir, f"dt={date_str}")

    if not os.path.exists(silver_path):
        raise FileNotFoundError(
            f"No validated data found for {date_str} at {silver_path}. "
            f"Has the ingest stage been run for this date?"
        )

    os.makedirs(gold_path_dir, exist_ok=True)

    revenue_by_category = defaultdict(float)
    purchase_count_by_product = defaultdict(int)
    funnel_counts = {"page_view": 0, "add_to_cart": 0, "purchase": 0}

    with open(silver_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            event_type = event["event_type"]

            funnel_counts[event_type] = funnel_counts.get(event_type, 0) + 1

            if event_type == "purchase":
                category = event["product_category"]
                revenue = event["unit_price"] * event["quantity"]
                revenue_by_category[category] += revenue
                purchase_count_by_product[event["product_id"]] += event["quantity"]

    top_products = sorted(purchase_count_by_product.items(), key=lambda kv: kv[1], reverse=True)[:10]

    metrics = {
        "date": date_str,
        "revenue_by_category": {k: round(v, 2) for k, v in revenue_by_category.items()},
        "total_revenue": round(sum(revenue_by_category.values()), 2),
        "top_products": [{"product_id": pid, "units_sold": units} for pid, units in top_products],
        "funnel": funnel_counts,
        "conversion_rate_view_to_purchase": (
            round(funnel_counts["purchase"] / funnel_counts["page_view"], 4)
            if funnel_counts.get("page_view") else 0.0
        ),
    }

    # Overwrite this date's metrics file — see module docstring on idempotency.
    metrics_file = os.path.join(gold_path_dir, "metrics.json")
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform validated events into daily business metrics")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    result = transform_date(args.date)
    print(json.dumps(result, indent=2))
