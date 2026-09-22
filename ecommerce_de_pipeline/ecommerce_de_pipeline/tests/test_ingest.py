"""
tests/test_ingest.py

WHY THESE SPECIFIC TESTS:
A senior data engineer doesn't just test that the "happy path" works — they
test the two properties that matter most for a batch pipeline:
  1. Validation correctly separates good data from bad data, for each kind
     of bad data you expect to see (this test suite mirrors the corruption
     types in generate_events.py).
  2. The stage is genuinely idempotent: running it twice on the same input
     produces byte-for-byte the same output. This is the property that
     lets you safely re-run a failed or fixed job in production without
     fear of corrupting your data.

These tests build their own tiny, fully controlled bronze dataset rather
than depending on generate_events.py's randomness — a good test should be
deterministic and not rely on another script's random output.
"""

import json
import os
import shutil
import tempfile

import pytest

from pipeline.ingest import ingest_date


@pytest.fixture
def tmp_dirs():
    base = tempfile.mkdtemp()
    dirs = {
        "bronze_dir": os.path.join(base, "bronze"),
        "silver_dir": os.path.join(base, "silver"),
        "quarantine_dir": os.path.join(base, "quarantine"),
    }
    yield dirs
    shutil.rmtree(base)


def write_bronze(bronze_dir: str, date_str: str, events: list[dict]):
    partition_dir = os.path.join(bronze_dir, f"dt={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    with open(os.path.join(partition_dir, "events.jsonl"), "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def make_valid_event(event_id="e1", event_type="page_view", **overrides):
    event = {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": "2026-09-22T10:00:00",
        "user_id": "u1",
        "session_id": "s1",
        "product_id": "P100",
        "product_category": "electronics",
    }
    if event_type in ("add_to_cart", "purchase"):
        event["quantity"] = 1
    if event_type == "purchase":
        event["unit_price"] = 9.99
    event.update(overrides)
    return event


def test_valid_events_pass_through(tmp_dirs):
    events = [make_valid_event(event_id="e1"), make_valid_event(event_id="e2", event_type="purchase")]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    summary = ingest_date("2026-09-22", **tmp_dirs)

    assert summary["valid_rows"] == 2
    assert summary["quarantined_rows"] == 0


def test_missing_required_field_is_quarantined(tmp_dirs):
    events = [make_valid_event(event_id="e1", user_id="")]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    summary = ingest_date("2026-09-22", **tmp_dirs)

    assert summary["valid_rows"] == 0
    assert summary["quarantined_rows"] == 1
    assert "missing_or_empty_field:user_id" in summary["quarantine_reasons"]


def test_bad_timestamp_is_quarantined(tmp_dirs):
    events = [make_valid_event(event_id="e1", timestamp="not-a-date")]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    summary = ingest_date("2026-09-22", **tmp_dirs)

    assert summary["quarantine_reasons"].get("unparseable_timestamp") == 1


def test_negative_price_is_quarantined(tmp_dirs):
    events = [make_valid_event(event_id="e1", event_type="purchase", unit_price=-5.0)]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    summary = ingest_date("2026-09-22", **tmp_dirs)

    assert summary["quarantine_reasons"].get("invalid_unit_price") == 1


def test_duplicate_event_id_is_deduplicated(tmp_dirs):
    events = [make_valid_event(event_id="e1"), make_valid_event(event_id="e1")]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    summary = ingest_date("2026-09-22", **tmp_dirs)

    assert summary["valid_rows"] == 1
    assert summary["duplicate_rows"] == 1


def test_ingest_is_idempotent(tmp_dirs):
    """Running the same day's ingestion twice must produce identical output
    — this is the property that makes re-running a fixed/retried job safe."""
    events = [make_valid_event(event_id="e1"), make_valid_event(event_id="e2", event_type="purchase")]
    write_bronze(tmp_dirs["bronze_dir"], "2026-09-22", events)

    first_summary = ingest_date("2026-09-22", **tmp_dirs)
    silver_path = os.path.join(tmp_dirs["silver_dir"], "dt=2026-09-22", "events.jsonl")
    with open(silver_path) as f:
        first_contents = f.read()

    second_summary = ingest_date("2026-09-22", **tmp_dirs)
    with open(silver_path) as f:
        second_contents = f.read()

    assert first_summary["valid_rows"] == second_summary["valid_rows"]
    assert first_contents == second_contents
