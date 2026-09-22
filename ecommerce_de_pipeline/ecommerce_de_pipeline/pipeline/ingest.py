"""
pipeline/ingest.py

Stage 1 of the pipeline: bronze (raw) -> silver (validated).

WHAT THIS STAGE IS RESPONSIBLE FOR:
  - Reading every raw event for a given date partition.
  - Validating each event against the schema (pipeline/schema.py).
  - Removing duplicate event_ids (upstream systems often deliver the same
    event more than once — see generate_events.py for why).
  - Writing the clean, valid rows to the "silver" layer.
  - Writing the rejected rows to a "quarantine" area, tagged with *why*
    they were rejected, instead of just dropping them silently.

WHY VALIDATION IS ITS OWN STAGE, SEPARATE FROM TRANSFORM:
It's tempting to just filter bad data out inline while computing your
aggregates. Real teams deliberately don't do this, for a few reasons:
  1. Separation of concerns: "is this data trustworthy" and "what does this
     data mean for the business" are different questions, and mixing them
     makes both harder to reason about and to test.
  2. Debuggability: if revenue numbers look wrong tomorrow, you want to be
     able to ask "was it a data quality problem or a business-logic bug?"
     in isolation. A quarantine table with reasons attached answers that in
     seconds instead of hours.
  3. Auditability: quarantined rows are a paper trail. If someone asks "why
     did we undercount purchases on March 3rd", you can go look.

WHY THIS STAGE IS IDEMPOTENT (safe to re-run):
This function always *overwrites* the silver/quarantine output for a given
date rather than appending to it. That means if the pipeline fails halfway
through, or you need to re-run today's batch because of a bug fix, running
it again produces the same correct result — not a doubled or corrupted one.
This "overwrite the partition, don't append" pattern is one of the most
important habits in batch data engineering (it's what dbt's "incremental"
strategy and Spark's "insertInto with overwrite" are built to formalize).
"""

import json
import os
from collections import Counter

from pipeline.schema import validate_event


def ingest_date(date_str: str, bronze_dir: str = "data/bronze", silver_dir: str = "data/silver",
                 quarantine_dir: str = "data/quarantine") -> dict:
    bronze_path = os.path.join(bronze_dir, f"dt={date_str}", "events.jsonl")
    silver_path_dir = os.path.join(silver_dir, f"dt={date_str}")
    quarantine_path_dir = os.path.join(quarantine_dir, f"dt={date_str}")

    if not os.path.exists(bronze_path):
        raise FileNotFoundError(
            f"No raw data found for {date_str} at {bronze_path}. "
            f"Has generate_events.py been run for this date?"
        )

    os.makedirs(silver_path_dir, exist_ok=True)
    os.makedirs(quarantine_path_dir, exist_ok=True)

    valid_rows = []
    quarantined_rows = []
    seen_event_ids = set()
    duplicate_count = 0
    reason_counts = Counter()

    with open(bronze_path, "r") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                quarantined_rows.append({"raw_line": line, "reason": "invalid_json", "line_number": line_number})
                reason_counts["invalid_json"] += 1
                continue

            # Duplicate check happens before schema validation: a duplicate
            # of a perfectly valid event is still not something we want to
            # double-count in our business metrics.
            event_id = event.get("event_id")
            if event_id and event_id in seen_event_ids:
                duplicate_count += 1
                quarantined_rows.append({**event, "reason": "duplicate_event_id"})
                reason_counts["duplicate_event_id"] += 1
                continue

            is_valid, reason = validate_event(event)
            if is_valid:
                valid_rows.append(event)
                if event_id:
                    seen_event_ids.add(event_id)
            else:
                quarantined_rows.append({**event, "reason": reason})
                reason_counts[reason] += 1

    # Overwrite (not append) — see the module docstring for why this matters.
    silver_file = os.path.join(silver_path_dir, "events.jsonl")
    with open(silver_file, "w") as f:
        for row in valid_rows:
            f.write(json.dumps(row) + "\n")

    quarantine_file = os.path.join(quarantine_path_dir, "events.jsonl")
    with open(quarantine_file, "w") as f:
        for row in quarantined_rows:
            f.write(json.dumps(row) + "\n")

    summary = {
        "date": date_str,
        "total_rows": len(valid_rows) + len(quarantined_rows),
        "valid_rows": len(valid_rows),
        "quarantined_rows": len(quarantined_rows),
        "duplicate_rows": duplicate_count,
        "quarantine_reasons": dict(reason_counts),
    }
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest and validate one day's raw events")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    result = ingest_date(args.date)
    print(json.dumps(result, indent=2))
