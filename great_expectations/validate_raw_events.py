"""
Great Expectations gate for the raw GH Archive landing zone, run before
load_to_bigquery.py in the DAG so a data quality regression fails the task
(and Airflow skips the downstream load) instead of pushing bad data into
BigQuery.

Validates, for one day's worth of already-ingested hourly files:
  - the expected top-level columns are present (id, type, actor, repo,
    payload, public, created_at) - `org` is allowed but not required, since
    it's only present on org-scoped events
  - the null rate on critical fields (id, type, created_at) stays under
    NULL_RATE_THRESHOLD
  - created_at falls within the target day (plus a small buffer for
    timezone/publish-boundary stragglers)
  - `type` is one of GH Archive's known event types (kept in sync with
    dbt_project/models/staging/_staging__models.yml's accepted_values test -
    this is the same check running twice: once here, fast, before a BigQuery
    load job is even attempted, and again in dbt after the load, as a second
    independent check on the transformed data)

At GH Archive's real daily volume (~2.5M events/day), loading every column -
including `payload` and `actor`/`repo`, which are nested objects on every
single row - into memory at once is what actually OOM'd this in development
even on the full-day scale we're validating here. Two checks run against two
differently-sized batches instead of one big one:
  - column presence is checked against a SAMPLE of records, not all of them.
    This is safe, not just fast: id/type/actor/repo/payload/public/created_at
    are GH's fixed event envelope present on every event regardless of type,
    so if the schema were broken it'd show up in the first record just as
    reliably as the two-millionth.
  - the per-row threshold checks (null rate, timestamp range, known type) run
    against the full day, but only the three lightweight columns they
    actually need - never the heavy nested payload/actor/repo columns.

Usage:
    python validate_raw_events.py --date 2026-08-06 --raw-dir raw
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import great_expectations as gx
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from load_to_bigquery import day_source_files  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("validate_raw_events")

GE_ROOT_DIR = Path(__file__).resolve().parent

KNOWN_EVENT_TYPES = [
    "CommitCommentEvent",
    "CreateEvent",
    "DeleteEvent",
    "DiscussionEvent",
    "DiscussionCommentEvent",
    "ForkEvent",
    "GollumEvent",
    "IssueCommentEvent",
    "IssuesEvent",
    "MemberEvent",
    "PublicEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "PullRequestReviewThreadEvent",
    "PushEvent",
    "ReleaseEvent",
    "SponsorshipEvent",
    "WatchEvent",
]

REQUIRED_COLUMNS = ["id", "type", "actor", "repo", "payload", "public", "created_at"]
CRITICAL_NOT_NULL_COLUMNS = ["id", "type", "created_at"]
LIGHT_COLUMNS = ["id", "type", "created_at"]
NULL_RATE_THRESHOLD = 0.01  # fail if more than 1% of a critical field is null
TIMESTAMP_BUFFER_HOURS = 6  # tolerate stragglers near the day boundary
SCHEMA_SAMPLE_SIZE = 2000


def sample_records(raw_dir: Path, day: date, n: int) -> list[dict]:
    """A small, bounded sample of raw records, read with plain json.loads
    since n is capped - unlike the full-day load, this is never large enough
    to matter for speed or memory.
    """
    files = day_source_files(raw_dir, day)
    if not files:
        raise FileNotFoundError(f"no raw files found for {day} under {raw_dir}")

    records: list[dict] = []
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
                    if len(records) >= n:
                        return records
    return records


def load_light_dataframe(raw_dir: Path, day: date) -> pd.DataFrame:
    """The full day's id/type/created_at columns only, parsed with pandas'
    own (C-based, columnar) JSON reader rather than a Python-level json.loads
    loop - at GH Archive's daily volume, materializing a Python dict per
    record first is slow enough to be impractical.
    """
    files = day_source_files(raw_dir, day)
    if not files:
        raise FileNotFoundError(f"no raw files found for {day} under {raw_dir}")

    frames = [pd.read_json(path, lines=True, compression="gzip")[LIGHT_COLUMNS] for path in files]
    df = pd.concat(frames, ignore_index=True)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    return df


def _get_or_create_asset(datasource, name: str):
    try:
        return datasource.add_dataframe_asset(name=name)
    except gx.exceptions.DataContextError:
        return datasource.get_asset(name)


def _run_checkpoint(context, datasource, asset_name: str, suite_name: str, df: pd.DataFrame, build_suite) -> bool:
    data_asset = _get_or_create_asset(datasource, asset_name)
    batch_request = data_asset.build_batch_request(dataframe=df)

    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    suite.expectations = []  # reset so re-runs don't accumulate duplicate expectations

    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    build_suite(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(name=f"{suite_name}_checkpoint", validator=validator)
    result = checkpoint.run()

    if not result.success:
        for validation_result in result.list_validation_results():
            for res in validation_result["results"]:
                if not res["success"]:
                    logger.error(
                        "FAILED expectation %s: %s",
                        res["expectation_config"]["expectation_type"],
                        res["result"],
                    )

    return bool(result.success)


def run_validation(sample_df: pd.DataFrame, light_df: pd.DataFrame, day: date) -> bool:
    context = gx.get_context(mode="file", project_root_dir=str(GE_ROOT_DIR))
    datasource = context.sources.add_or_update_pandas("raw_gharchive")

    def build_schema_suite(validator):
        validator.expect_table_columns_to_match_set(column_set=REQUIRED_COLUMNS, exact_match=False)

    def build_quality_suite(validator):
        for column in CRITICAL_NOT_NULL_COLUMNS:
            validator.expect_column_values_to_not_be_null(column, mostly=1 - NULL_RATE_THRESHOLD)

        range_start = pd.Timestamp(day, tz="UTC") - pd.Timedelta(hours=TIMESTAMP_BUFFER_HOURS)
        range_end = pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1, hours=TIMESTAMP_BUFFER_HOURS)
        validator.expect_column_values_to_be_between(
            "created_at", min_value=range_start, max_value=range_end, mostly=1 - NULL_RATE_THRESHOLD
        )

        validator.expect_column_values_to_be_in_set(
            "type", KNOWN_EVENT_TYPES, mostly=1 - NULL_RATE_THRESHOLD
        )

    schema_ok = _run_checkpoint(context, datasource, "schema_sample", "raw_gharchive_schema", sample_df, build_schema_suite)
    quality_ok = _run_checkpoint(context, datasource, "quality_full_day", "raw_gharchive_quality", light_df, build_quality_suite)

    return schema_ok and quality_ok


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a day of raw GH Archive files with Great Expectations.")
    parser.add_argument("--date", type=str, required=True, help="Date to validate, YYYY-MM-DD.")
    parser.add_argument("--raw-dir", type=str, default="raw")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    day = datetime.strptime(args.date, "%Y-%m-%d").date()

    try:
        sample_df = pd.DataFrame.from_records(sample_records(Path(args.raw_dir), day, SCHEMA_SAMPLE_SIZE))
        light_df = load_light_dataframe(Path(args.raw_dir), day)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    logger.info("validating %d raw events for %s (schema sample: %d)", len(light_df), day, len(sample_df))
    success = run_validation(sample_df, light_df, day)

    if not success:
        logger.error("data quality validation FAILED for %s", day)
        return 1

    logger.info("data quality validation passed for %s", day)
    return 0


if __name__ == "__main__":
    sys.exit(main())
