"""
pipeline/schema.py

Defines what a valid event looks like, and the validation function used by
the ingestion stage.

WHY THIS FILE EXISTS (data engineering context):
In a real company, the "schema" for an event is a contract between whoever
produces the data (the app / website / mobile team) and whoever consumes it
(you, the data engineer). Producers change their code without warning you,
so your pipeline needs to actively CHECK that contract on every run rather
than assume the data will always look the way it did yesterday.

Real teams usually formalize this further with a schema registry (e.g.
Confluent Schema Registry for Kafka) or a validation library like Pydantic
or Great Expectations. We're writing the checks by hand here, in plain
Python, so you can see exactly what "schema validation" actually means
under the hood before you reach for a library that does it for you.
"""

from datetime import datetime

# The set of event types this pipeline understands. If a new event type
# shows up that isn't in this list, we treat it as invalid rather than
# silently accepting it — an unrecognized event type is often a sign that
# an upstream team shipped a change nobody told you about.
VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}

# Fields every event must have, regardless of event_type.
REQUIRED_FIELDS = ["event_id", "event_type", "timestamp", "user_id", "session_id", "product_id"]


def validate_event(event: dict) -> tuple[bool, str | None]:
    """
    Check a single raw event dict against the schema.

    Returns (True, None) if valid.
    Returns (False, "<reason>") if invalid — the reason is stored alongside
    the row when we quarantine it, so a human can later look at *why* a
    batch of data was rejected instead of just seeing "invalid" with no
    explanation. Good error messages here save hours of debugging later.
    """
    # 1. Check all required fields are present and non-empty.
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] in (None, ""):
            return False, f"missing_or_empty_field:{field}"

    # 2. event_type must be one we recognize.
    if event["event_type"] not in VALID_EVENT_TYPES:
        return False, f"unknown_event_type:{event['event_type']}"

    # 3. timestamp must be a real, parseable ISO-8601 timestamp.
    try:
        datetime.fromisoformat(event["timestamp"])
    except (ValueError, TypeError):
        return False, "unparseable_timestamp"

    # 4. purchase events must have a sane, non-negative price and quantity.
    if event["event_type"] == "purchase":
        price = event.get("unit_price")
        qty = event.get("quantity")
        if not isinstance(price, (int, float)) or price < 0:
            return False, "invalid_unit_price"
        if not isinstance(qty, int) or qty <= 0:
            return False, "invalid_quantity"

    return True, None
