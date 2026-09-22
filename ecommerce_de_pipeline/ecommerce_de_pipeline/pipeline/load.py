"""
pipeline/load.py

Stage 3 of the pipeline: gold (per-day JSON metrics files) -> warehouse
(a queryable SQLite database).

WHY THIS STAGE EXISTS AT ALL:
Up to this point, everything lives as files on disk, partitioned by date.
That's great for the pipeline itself, but it's a bad interface for anyone
else — an analyst, a dashboard, a report — who wants to ask questions like
"show me revenue by category for the last 30 days" using plain SQL. This
stage's whole job is to load each day's file-based output into proper
tables so that question becomes one SQL query instead of a Python script
that reads 30 separate JSON files.

WHY SQLITE HERE (AND WHAT IT STANDS IN FOR):
In a real company this warehouse would usually be Postgres, Snowflake,
BigQuery, or Redshift — something multiple people and tools can connect to
at once. SQLite is a single local file, so it can't do that. But the SQL
you write against it, and the *pattern* of loading data into it, is nearly
identical to what you'd do against a real warehouse. Learning the pattern
here transfers directly; swapping SQLite for `psycopg2`/Postgres later is a
small, mechanical change, not a redesign.

WHY LOADING USES "DELETE THEN INSERT" PER DATE, NOT PLAIN INSERT:
If you re-run today's pipeline (say, after fixing a bug), a plain INSERT
would leave you with duplicate rows for that date sitting next to the
corrected ones. Instead, we first delete any existing rows for that exact
date, then insert the fresh ones, inside a single transaction. This is the
same idempotent-by-partition-overwrite idea used in every earlier stage,
just implemented as SQL instead of "rewrite a file." This exact pattern has
a name in the industry: it's often called an "upsert by partition" or, in
warehouse-specific language, a MERGE / INSERT OVERWRITE.
"""

import json
import os
import sqlite3


SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_revenue_by_category (
    date TEXT NOT NULL,
    category TEXT NOT NULL,
    revenue REAL NOT NULL,
    PRIMARY KEY (date, category)
);

CREATE TABLE IF NOT EXISTS daily_top_products (
    date TEXT NOT NULL,
    product_id TEXT NOT NULL,
    units_sold INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    PRIMARY KEY (date, product_id)
);

CREATE TABLE IF NOT EXISTS daily_funnel (
    date TEXT NOT NULL PRIMARY KEY,
    page_view INTEGER NOT NULL,
    add_to_cart INTEGER NOT NULL,
    purchase INTEGER NOT NULL,
    conversion_rate_view_to_purchase REAL NOT NULL,
    total_revenue REAL NOT NULL
);
"""


def get_connection(db_path: str = "warehouse.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def load_date(date_str: str, gold_dir: str = "data/gold", db_path: str = "warehouse.db") -> dict:
    metrics_path = os.path.join(gold_dir, f"dt={date_str}", "metrics.json")
    if not os.path.exists(metrics_path):
        raise FileNotFoundError(
            f"No gold metrics found for {date_str} at {metrics_path}. "
            f"Has the transform stage been run for this date?"
        )

    with open(metrics_path, "r") as f:
        metrics = json.load(f)

    conn = get_connection(db_path)
    try:
        # Everything for this date happens in one transaction: either all
        # of it lands, or none of it does. A partial write (e.g. revenue
        # rows saved but funnel row missing because the process crashed
        # mid-way) is worse than no write at all, because it's silently
        # wrong rather than obviously missing.
        with conn:
            conn.execute("DELETE FROM daily_revenue_by_category WHERE date = ?", (date_str,))
            conn.execute("DELETE FROM daily_top_products WHERE date = ?", (date_str,))
            conn.execute("DELETE FROM daily_funnel WHERE date = ?", (date_str,))

            for category, revenue in metrics["revenue_by_category"].items():
                conn.execute(
                    "INSERT INTO daily_revenue_by_category (date, category, revenue) VALUES (?, ?, ?)",
                    (date_str, category, revenue),
                )

            for rank, product in enumerate(metrics["top_products"], start=1):
                conn.execute(
                    "INSERT INTO daily_top_products (date, product_id, units_sold, rank) VALUES (?, ?, ?, ?)",
                    (date_str, product["product_id"], product["units_sold"], rank),
                )

            funnel = metrics["funnel"]
            conn.execute(
                """INSERT INTO daily_funnel
                   (date, page_view, add_to_cart, purchase, conversion_rate_view_to_purchase, total_revenue)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    date_str,
                    funnel.get("page_view", 0),
                    funnel.get("add_to_cart", 0),
                    funnel.get("purchase", 0),
                    metrics["conversion_rate_view_to_purchase"],
                    metrics["total_revenue"],
                ),
            )
    finally:
        conn.close()

    return {"date": date_str, "loaded_to": db_path}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load one day's gold metrics into the SQLite warehouse")
    parser.add_argument("--date", required=True)
    parser.add_argument("--db-path", default="warehouse.db")
    args = parser.parse_args()

    result = load_date(args.date, db_path=args.db_path)
    print(json.dumps(result, indent=2))
