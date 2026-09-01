"""Daily batch ELT DAG for GH Archive events.

Runs once per day, well after GH Archive has published every hour in the
prior day's data interval. See README.md "Status" for what's wired up so far.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from alerts import alert_on_failure

RAW_DIR = "/opt/airflow/raw"
INGESTION_SCRIPT = "/opt/airflow/ingestion/extract_gharchive.py"
VALIDATE_SCRIPT = "/opt/airflow/great_expectations/validate_raw_events.py"
LOAD_SCRIPT = "/opt/airflow/ingestion/load_to_bigquery.py"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="daily_elt_dag",
    description="Ingest, load, transform, and validate GH Archive events daily.",
    schedule="0 3 * * *",  # 03:00 UTC: the prior day's hours are safely published by then
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["elt", "gharchive"],
)
def daily_elt_dag():

    @task.bash
    def extract_gharchive() -> str:
        return (
            f"python {INGESTION_SCRIPT} "
            "--start {{ data_interval_start.to_iso8601_string() }} "
            "--end {{ data_interval_end.to_iso8601_string() }} "
            f"--raw-dir {RAW_DIR}"
        )

    @task.bash
    def validate_raw_events() -> str:
        return (
            f"python {VALIDATE_SCRIPT} "
            "--date {{ data_interval_start.strftime('%Y-%m-%d') }} "
            f"--raw-dir {RAW_DIR}"
        )

    @task.bash
    def load_to_bigquery() -> str:
        return (
            f"python {LOAD_SCRIPT} "
            "--date {{ data_interval_start.strftime('%Y-%m-%d') }} "
            f"--raw-dir {RAW_DIR}"
        )

    # --target prod: this is the actual operational pipeline, not a PR
    # validation step (that's what dbt CI's dev target is for), and prod is
    # what Metabase reads from. Without this, the daily run would build the
    # dev target instead, and the dashboard would only refresh on the next
    # git merge (via cd.yml) rather than on the next day's data - silently
    # decoupling "the pipeline runs daily" from "the dashboard shows daily
    # data."
    @task.bash
    def dbt_run() -> str:
        return f"dbt run --target prod --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

    @task.bash
    def dbt_test() -> str:
        return f"dbt test --target prod --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

    # validate_raw_events fails (non-zero exit) on a data quality regression,
    # which stops load_to_bigquery and everything after it from running at all -
    # bad data never reaches BigQuery in the first place.
    extract_gharchive() >> validate_raw_events() >> load_to_bigquery() >> dbt_run() >> dbt_test()


daily_elt_dag()
