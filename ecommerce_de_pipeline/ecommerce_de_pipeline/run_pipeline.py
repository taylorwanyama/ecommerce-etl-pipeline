"""
run_pipeline.py

The orchestrator. Runs ingest -> transform -> load, in that order, for one
date, and stops immediately if any stage fails.

WHAT "ORCHESTRATION" MEANS IN THE REAL WORLD:
In a real company, this job would usually be scheduled and run by a tool
like Airflow, Prefect, or Dagster rather than a plain Python script you run
by hand. Those tools add things this script deliberately doesn't try to
reimplement: automatic retries with backoff, alerting when a run fails,
a UI showing run history, backfilling a date range, and managing
dependencies between many pipelines, not just one.

But underneath all of that tooling, the core idea is exactly what's
below: a DAG (directed acyclic graph) of tasks — ingest depends on raw
data existing, transform depends on ingest having succeeded, load depends
on transform having succeeded — where a failure at any step stops
everything downstream of it. Writing that dependency chain by hand once,
here, is what makes an orchestrator's DAG view make immediate sense the
first time you open one.

WHY THE PIPELINE PROCESSES "YESTERDAY" BY DEFAULT, NOT "TODAY":
Real batch pipelines almost always process a *complete* day of data, which
means the day that just finished, not the one still in progress. A batch
job scheduled to run at 1am and process "today" would be reading a day
that's only an hour old — obviously incomplete. Defaulting to yesterday
(T-1) mirrors how production batch jobs are actually scheduled.

WHY EACH STAGE'S FAILURE STOPS THE WHOLE RUN:
If ingestion silently fails but transform runs anyway, you get a gold
metrics file confidently reporting numbers computed from partial or missing
data — which is far more dangerous than the pipeline simply not producing
a number at all. Fail loudly and stop, rather than fail quietly and
continue, is one of the most important defensive habits in data
engineering.
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta

from pipeline.ingest import ingest_date
from pipeline.transform import transform_date
from pipeline.load import load_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_runs.log"),
    ],
)
logger = logging.getLogger("run_pipeline")


def run_stage(stage_name: str, stage_fn, *args, **kwargs):
    """Run one pipeline stage with timing and consistent error handling.

    This small wrapper is what a "task" looks like inside a real
    orchestrator: log the start, measure duration, log success/failure, and
    let the caller decide what happens next on failure.
    """
    logger.info(f"START  {stage_name}")
    start = time.time()
    try:
        result = stage_fn(*args, **kwargs)
    except Exception as exc:
        elapsed = time.time() - start
        logger.error(f"FAILED {stage_name} after {elapsed:.2f}s: {exc}")
        raise
    elapsed = time.time() - start
    logger.info(f"DONE   {stage_name} in {elapsed:.2f}s | {result}")
    return result


def run_pipeline(date_str: str) -> None:
    logger.info(f"=== Pipeline run starting for date={date_str} ===")

    try:
        ingest_summary = run_stage("ingest", ingest_date, date_str)

        # A basic data-quality gate: if almost everything got quarantined,
        # something is badly wrong upstream (e.g. a producer shipped a
        # breaking schema change). A real pipeline would alert a human here
        # rather than silently continuing to produce gold metrics off a
        # tiny, unrepresentative sample of the day's data.
        total = ingest_summary["total_rows"]
        valid = ingest_summary["valid_rows"]
        if total > 0 and (valid / total) < 0.5:
            raise RuntimeError(
                f"Data quality gate failed: only {valid}/{total} rows were valid for {date_str}. "
                f"Stopping before transform/load. Reasons: {ingest_summary['quarantine_reasons']}"
            )

        run_stage("transform", transform_date, date_str)
        run_stage("load", load_date, date_str)

    except Exception:
        logger.error(f"=== Pipeline run FAILED for date={date_str}, downstream stages were skipped ===")
        sys.exit(1)

    logger.info(f"=== Pipeline run SUCCEEDED for date={date_str} ===")


def main():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="Run the full ingest -> transform -> load pipeline for one date")
    parser.add_argument("--date", default=yesterday, help="Date to process, YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args()

    run_pipeline(args.date)


if __name__ == "__main__":
    main()
