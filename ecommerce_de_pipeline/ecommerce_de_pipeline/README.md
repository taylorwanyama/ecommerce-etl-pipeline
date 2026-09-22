# E-commerce Event Pipeline

A small, fully self-contained batch data engineering project: raw
e-commerce events (page views, add-to-carts, purchases) get validated,
transformed into business metrics, and loaded into a queryable warehouse —
following the same patterns real data engineering teams use, just at a
scale you can run on a laptop.

## Why this project

Most "learn data engineering" tutorials teach you syntax (how to write a
SQL query, how to use pandas) without teaching you the *judgment calls*
that separate a fragile script from a production pipeline. This project is
built around those judgment calls, and every non-obvious decision below is
explained in the code comments where it's made, not just here.

## Architecture

```
generate_events.py                (simulates the upstream app — not part
        |                          of the pipeline itself)
        v
data/bronze/dt=YYYY-MM-DD/        <- raw, immutable, exactly as received
        |
        v   pipeline/ingest.py     (validate, dedupe, quarantine bad rows)
        |
data/silver/dt=YYYY-MM-DD/        <- clean, trustworthy, row-level events
        |
        v   pipeline/transform.py  (business logic: revenue, funnel, etc.)
        |
data/gold/dt=YYYY-MM-DD/          <- aggregated daily business metrics
        |
        v   pipeline/load.py       (upsert into SQLite)
        |
warehouse.db                      <- what analysts/dashboards actually query
```

`run_pipeline.py` is the orchestrator: it runs ingest -> transform -> load,
in that order, for one date, and stops immediately if any stage fails.

This bronze / silver / gold naming is a real industry convention — often
called the "medallion architecture" (popularized by Databricks, but the
underlying idea — raw, then cleaned, then aggregated — predates the name
and shows up under different labels almost everywhere: dbt calls it
staging -> marts, a classic data warehouse calls it ODS -> facts/dims).

## The decisions that matter (and why)

**1. Raw data is immutable and never modified in place.**
`data/bronze/` is written once by the generator and never touched again by
the pipeline. If a bug in `transform.py` corrupts your metrics next month,
you can always re-run the whole pipeline from the untouched raw data and
get a correct result. Pipelines that transform data in place, with no raw
copy retained, have no way to recover from a bug except "hope the data
still exists somewhere upstream."

**2. Validation is a separate stage from business logic.**
`ingest.py` only asks "is this row trustworthy?" `transform.py` only asks
"what does this trustworthy data mean for the business?" Keeping these
separate means: (a) you can test them independently, (b) when a metric
looks wrong, you can immediately tell whether it's a data quality issue or
a business-logic bug by checking the quarantine reasons first, and (c) a
producer team can ship a schema change and you'll see it show up as
`unknown_event_type` in quarantine within one pipeline run, not as a silent
miscalculation three weeks later.

**3. Every stage is idempotent via "overwrite the partition," not append.**
Each stage fully rewrites its output for a given date every time it runs,
rather than appending to existing output. This is what makes it *safe* to
re-run a failed job, backfill a date after fixing a bug, or accidentally
trigger the same run twice — outcomes that happen constantly in real
production systems. Without this property, every retry is a small
disaster waiting to double-count something.

**4. Data is partitioned by date on disk (`dt=YYYY-MM-DD/`).**
This "Hive-style" partitioning convention lets you reprocess a single day
in isolation, and is exactly how tools like Spark, Athena, and Presto
expect data to be laid out on a real data lake. It's also what makes
backfilling a date range trivial: just loop over dates and call
`run_pipeline.py --date <date>` for each one.

**5. The pipeline has a data-quality gate, not just row-level validation.**
`run_pipeline.py` checks that at least 50% of a day's rows passed
validation before continuing to transform/load. Row-level validation
catches individual bad records; this catches the case where *most* of a
day's data is bad — usually the sign of a bigger upstream problem (a
breaking schema change, a broken producer) that no amount of per-row
quarantining should quietly paper over.

**6. Loading uses transactional delete-then-insert per date, not blind
inserts.**
`load.py` wraps each date's writes in a single SQLite transaction: delete
any existing rows for that date, then insert the new ones, all or nothing.
This is the local-file equivalent of the `MERGE` / `INSERT OVERWRITE`
patterns used in real warehouses, and it means a load can be safely
retried without manual cleanup.

**7. SQLite stands in for Postgres/Snowflake/BigQuery.**
The SQL and the *pattern* of loading transfer directly — swapping SQLite
for `psycopg2` and a real Postgres connection later is a small, mechanical
change to `load.py`, not a redesign of the pipeline.

## What's simplified here, on purpose

This project teaches the core patterns, but a real production version
would add:
- **A real orchestrator** (Airflow, Prefect, Dagster) instead of a plain
  Python script — for scheduling, automatic retries with backoff, a run
  history UI, and alerting on failure.
- **Distributed processing** (Spark, or a warehouse's native compute) once
  data no longer fits comfortably on one machine.
- **A schema registry** or a validation library like Pydantic/Great
  Expectations, instead of hand-written `if` checks.
- **dbt** for the transform layer, which adds testing, documentation, and
  lineage tracking on top of what `transform.py` does by hand here.
- **CI/CD** running the test suite automatically on every change.
- **Monitoring/alerting** (e.g. paging someone if the data-quality gate
  trips) instead of just logging and exiting.
- **Streaming**, if the business actually needs near-real-time metrics
  instead of a daily batch — a materially different architecture (Kafka +
  a stream processor), not just a faster version of this one.

Knowing *when* to add each of these — and not reaching for all of them on
day one — is itself a core data engineering skill. Every one of them adds
real operational complexity, and a senior engineer earns the right to add
them by first proving the simple version works.

## How to run it

```bash
# 1. Install test dependencies (only pytest is needed; everything else is stdlib)
pip install pytest --break-system-packages

# 2. Generate a day of synthetic raw data (simulates the upstream app)
python3 generate_events.py --date 2026-09-21 --num-events 5000

# 3. Run the full pipeline for that date
python3 run_pipeline.py --date 2026-09-21

# 4. Query the result
python3 -c "
import sqlite3
conn = sqlite3.connect('warehouse.db')
print(conn.execute('SELECT * FROM daily_funnel').fetchall())
"

# 5. Run the tests
python3 -m pytest tests/ -v
```

To try a backfill, generate and run a few more dates:
```bash
for d in 2026-09-18 2026-09-19 2026-09-20; do
  python3 generate_events.py --date $d
  python3 run_pipeline.py --date $d
done
```
Then query across all of them: `SELECT date, total_revenue FROM daily_funnel ORDER BY date;`

## Project layout

```
generate_events.py       # simulates the upstream app (not part of the pipeline)
run_pipeline.py           # orchestrator: ingest -> transform -> load
pipeline/
    schema.py             # event schema + validation rules
    ingest.py              # bronze -> silver
    transform.py            # silver -> gold
    load.py                  # gold -> warehouse.db
tests/
    test_ingest.py         # validation + idempotency tests
    test_transform.py        # aggregation correctness tests
data/                     # created when you run the scripts (bronze/silver/gold)
warehouse.db              # created on first load
pipeline_runs.log         # created on first pipeline run
```
