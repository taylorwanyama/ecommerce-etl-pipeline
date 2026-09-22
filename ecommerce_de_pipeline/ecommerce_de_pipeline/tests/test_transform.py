"""
tests/test_transform.py

WHY THESE TESTS:
Business logic bugs (an off-by-one in the funnel, a wrong revenue formula)
are far more common in practice than infrastructure bugs, and they're much
more expensive because they produce numbers that *look* plausible while
being wrong. These tests build a small, hand-computed dataset where we know
the exact expected revenue and funnel counts in advance, so any regression
in the aggregation logic is caught immediately.
"""

import json
import os
import shutil
import tempfile

import pytest

from pipeline.transform import transform_date


@pytest.fixture
def tmp_dirs():
    base = tempfile.mkdtemp()
    dirs = {"silver_dir": os.path.join(base, "silver"), "gold_dir": os.path.join(base, "gold")}
    yield dirs
    shutil.rmtree(base)


def write_silver(silver_dir: str, date_str: str, events: list[dict]):
    partition_dir = os.path.join(silver_dir, f"dt={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    with open(os.path.join(partition_dir, "events.jsonl"), "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def test_revenue_and_funnel_are_computed_correctly(tmp_dirs):
    events = [
        {"event_type": "page_view", "product_category": "electronics", "product_id": "P1"},
        {"event_type": "page_view", "product_category": "electronics", "product_id": "P1"},
        {"event_type": "add_to_cart", "product_category": "electronics", "product_id": "P1", "quantity": 1},
        {"event_type": "purchase", "product_category": "electronics", "product_id": "P1",
         "quantity": 2, "unit_price": 10.0},
        {"event_type": "purchase", "product_category": "home", "product_id": "P2",
         "quantity": 1, "unit_price": 5.0},
    ]
    write_silver(tmp_dirs["silver_dir"], "2026-09-22", events)

    metrics = transform_date("2026-09-22", **tmp_dirs)

    assert metrics["revenue_by_category"]["electronics"] == 20.0
    assert metrics["revenue_by_category"]["home"] == 5.0
    assert metrics["total_revenue"] == 25.0
    assert metrics["funnel"]["page_view"] == 2
    assert metrics["funnel"]["add_to_cart"] == 1
    assert metrics["funnel"]["purchase"] == 2
    assert metrics["conversion_rate_view_to_purchase"] == 1.0  # 2 purchases / 2 page_views


def test_top_products_ranked_by_units_sold(tmp_dirs):
    events = [
        {"event_type": "purchase", "product_category": "electronics", "product_id": "P1",
         "quantity": 5, "unit_price": 10.0},
        {"event_type": "purchase", "product_category": "electronics", "product_id": "P2",
         "quantity": 1, "unit_price": 10.0},
    ]
    write_silver(tmp_dirs["silver_dir"], "2026-09-22", events)

    metrics = transform_date("2026-09-22", **tmp_dirs)

    assert metrics["top_products"][0]["product_id"] == "P1"
    assert metrics["top_products"][0]["units_sold"] == 5


def test_transform_is_idempotent(tmp_dirs):
    events = [{"event_type": "purchase", "product_category": "electronics", "product_id": "P1",
               "quantity": 1, "unit_price": 9.99}]
    write_silver(tmp_dirs["silver_dir"], "2026-09-22", events)

    first = transform_date("2026-09-22", **tmp_dirs)
    second = transform_date("2026-09-22", **tmp_dirs)

    assert first == second
