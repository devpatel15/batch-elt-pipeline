"""
Automates Metabase setup end to end via its REST API, so the dashboard is
reproducible from a fresh `docker compose up` rather than a one-time manual
click-through: creates the admin account, connects BigQuery (scoped to the
dbt marts dataset), builds the four charts, and assembles them into a
dashboard.

Idempotent: safe to re-run against an already-configured Metabase instance -
it reuses the existing admin session, database connection, cards, and
dashboard by name instead of creating duplicates each time.

Usage:
    python scripts/setup_metabase.py

Environment variables:
    METABASE_URL              default http://localhost:3000
    METABASE_ADMIN_EMAIL      default admin@example.com
    METABASE_ADMIN_PASSWORD   required on first run (no existing admin)
    GCP_PROJECT_ID            required
    GOOGLE_APPLICATION_CREDENTIALS   path to the service account JSON key
    MARTS_DATASET             default dbt_prod_marts

Points at dbt_prod_marts, not dbt_dev_marts: the dashboard is meant to reflect
what's actually live, and dev is where PR CI freely rebuilds (as views) on
every push, including ones that haven't merged yet. Pointing Metabase at prod
is what makes the dev/prod split actually protect the dashboard - the dataset
separation alone doesn't do that if the "production" tool reads from dev.
"""

from __future__ import annotations

import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("setup_metabase")

BASE = os.environ.get("METABASE_URL", "http://localhost:3000") + "/api"
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
MARTS_DATASET = os.environ.get("MARTS_DATASET", "dbt_prod_marts")

DATABASE_NAME = "GH Archive BigQuery"
DASHBOARD_NAME = "GH Archive Activity"

# A deliberate, non-default palette - Metabase's OSS chart colors ship as
# whatever the fresh-install default is (yellow/purple/blue in older
# versions), and leaving them untouched is one of the more obvious tells that
# nobody actually designed the dashboard. Global whitelabel color theming
# needs an Enterprise license, so these are applied per-card instead via
# series_settings / pie.colors - same visual result, works in OSS.
INDIGO = "#4F46E5"
TEAL = "#0D9488"
VIOLET = "#7C3AED"
SLATE = "#64748B"

CARDS = [
    {
        "name": "Daily Event Volume",
        "display": "line",
        "sql": f"""
            SELECT activity_date, SUM(total_events) AS total_events
            FROM `{MARTS_DATASET}.agg_daily_repo_activity`
            GROUP BY activity_date
            ORDER BY activity_date
        """,
        "viz_settings": {
            "graph.dimensions": ["activity_date"],
            "graph.metrics": ["total_events"],
            "series_settings": {"total_events": {"color": INDIGO}},
        },
    },
    {
        "name": "Top 10 Repos by Activity",
        "display": "bar",
        "sql": f"""
            SELECT r.repo_name, SUM(a.total_events) AS total_events
            FROM `{MARTS_DATASET}.agg_daily_repo_activity` a
            JOIN `{MARTS_DATASET}.dim_repo` r ON a.repo_id = r.repo_id
            GROUP BY r.repo_name
            ORDER BY total_events DESC
            LIMIT 10
        """,
        "viz_settings": {
            "graph.dimensions": ["repo_name"],
            "graph.metrics": ["total_events"],
            "series_settings": {"total_events": {"color": TEAL}},
        },
    },
    {
        "name": "Most Active Hours (UTC)",
        "display": "bar",
        "sql": f"""
            SELECT EXTRACT(HOUR FROM created_at) AS hour_of_day, COUNT(*) AS event_count
            FROM `{MARTS_DATASET}.fct_events`
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """,
        # Metabase's auto-viz inference only guesses axes reliably when the
        # dimension is a text column (works fine for repo_name above); a
        # numeric dimension like an hour-of-day integer left it unable to
        # guess, rendering "Which fields do you want to use for the X and Y
        # axes?" instead of a chart. Explicit dimensions/metrics sidesteps
        # that guesswork entirely, so every bar/line card gets them regardless
        # of column type.
        "viz_settings": {
            "graph.dimensions": ["hour_of_day"],
            "graph.metrics": ["event_count"],
            "series_settings": {"event_count": {"color": VIOLET}},
        },
    },
    {
        "name": "Event Type Breakdown",
        "display": "pie",
        "sql": f"""
            SELECT event_type, COUNT(*) AS event_count
            FROM `{MARTS_DATASET}.fct_events`
            GROUP BY event_type
            ORDER BY event_count DESC
        """,
        "viz_settings": {
            "pie.dimension": "event_type",
            "pie.metric": "event_count",
            "pie.colors": {"PushEvent": INDIGO, "CreateEvent": TEAL, "Other": SLATE},
        },
    },
]

# Same underlying tables as the charts above, just different aggregate
# views - single-number "scalar" cards give a dashboard viewer something to
# read in the first second, before they even look at a chart.
SCALAR_CARDS = [
    {
        "name": "Total Events Tracked",
        "sql": f"SELECT SUM(total_events) AS total_events FROM `{MARTS_DATASET}.agg_daily_repo_activity`",
    },
    {
        "name": "Repositories Tracked",
        "sql": f"SELECT COUNT(*) AS repo_count FROM `{MARTS_DATASET}.dim_repo`",
    },
    {
        "name": "Contributors Tracked",
        "sql": f"SELECT COUNT(*) AS actor_count FROM `{MARTS_DATASET}.dim_actor`",
    },
    {
        # Not pull_requests_merged: GH Archive's own PullRequestEvent payload
        # only carries a minimal pull_request sub-object (id/url/base/head),
        # without the `merged`/additions/deletions fields int_events_flattened
        # tries to extract - so that column is always null, not a bug
        # introduced here. pull_request_events (a plain event-type count, no
        # payload extraction involved) is real and populated.
        "name": "Pull Request Activity",
        "sql": f"SELECT SUM(pull_request_events) AS pr_events FROM `{MARTS_DATASET}.agg_daily_repo_activity`",
    },
]

HEADER_TEXT = """# GH Archive Activity

Daily snapshot of public GitHub activity, ingested from the [GH Archive](https://www.gharchive.org/) event stream.

**Pipeline:** Airflow extracts hourly archives -> validates with Great Expectations -> loads to BigQuery -> transforms with dbt. Runs daily at 03:00 UTC.
"""


def wait_for_metabase(timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if requests.get(f"{BASE}/health", timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError("Metabase did not become healthy in time")


def get_session() -> str:
    """Log in as admin, running first-time setup if no admin exists yet."""
    props = requests.get(f"{BASE}/session/properties").json()

    if not props.get("has-user-setup"):
        if not ADMIN_PASSWORD:
            raise RuntimeError("METABASE_ADMIN_PASSWORD is required for first-time setup")
        logger.info("no admin user yet, running first-time setup")
        resp = requests.post(
            f"{BASE}/setup",
            json={
                "token": props["setup-token"],
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                    "site_name": "GH Archive ELT",
                },
                "prefs": {"site_name": "GH Archive ELT", "site_locale": "en", "allow_tracking": False},
            },
        )
        resp.raise_for_status()
        return resp.json()["id"]

    logger.info("admin user already exists, logging in")
    resp = requests.post(f"{BASE}/session", json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    resp.raise_for_status()
    return resp.json()["id"]


def get_or_create_database(headers: dict) -> int:
    dbs = requests.get(f"{BASE}/database", headers=headers).json()["data"]
    existing = next((d for d in dbs if d["name"] == DATABASE_NAME), None)

    with open(CREDENTIALS_PATH) as f:
        sa_json = f.read()

    details = {
        "project-id": GCP_PROJECT_ID,
        "service-account-json": sa_json,
        "dataset-filters-type": "inclusion",
        "dataset-filters-patterns": MARTS_DATASET,
    }

    if existing:
        # Re-applied every run so a change to MARTS_DATASET (e.g. the
        # dev->prod repoint) reaches an already-configured instance instead
        # of only affecting brand-new setups.
        db_id = existing["id"]
        if existing["details"].get("dataset-filters-patterns") != MARTS_DATASET:
            logger.info("database %r exists but points at a different dataset, updating it", DATABASE_NAME)
            requests.put(f"{BASE}/database/{db_id}", headers=headers, json={"details": details}).raise_for_status()
            requests.post(f"{BASE}/database/{db_id}/sync_schema", headers=headers).raise_for_status()
        else:
            logger.info("database %r already exists (id=%s)", DATABASE_NAME, db_id)
        return db_id

    logger.info("creating database %r", DATABASE_NAME)
    resp = requests.post(
        f"{BASE}/database",
        headers=headers,
        json={"engine": "bigquery-cloud-sdk", "name": DATABASE_NAME, "details": details},
    )
    resp.raise_for_status()
    db_id = resp.json()["id"]

    for _ in range(24):
        tables = requests.get(f"{BASE}/database/{db_id}/metadata", headers=headers).json().get("tables", [])
        if tables:
            logger.info("synced %d tables: %s", len(tables), [t["name"] for t in tables])
            break
        time.sleep(5)
    else:
        logger.warning("database created but tables hadn't synced after 2 minutes; charts may be empty initially")

    return db_id


def upsert_cards(headers: dict, db_id: int, specs: list[dict], display: str | None = None) -> dict[str, int]:
    """Create or update a list of card specs by name, returning {name: card_id}.

    Re-applies the query/display/viz_settings on every run (not just at
    creation) so a fix - like the axis-mapping fix this self-healing update
    picked up originally - propagates to an already-configured instance
    instead of only affecting brand-new setups.
    """
    existing_cards = {c["name"]: c["id"] for c in requests.get(f"{BASE}/card", headers=headers).json()}

    ids = {}
    for spec in specs:
        body = {
            "name": spec["name"],
            "dataset_query": {"type": "native", "native": {"query": spec["sql"]}, "database": db_id},
            "display": spec.get("display", display),
            "visualization_settings": spec.get("viz_settings", {}),
        }

        if spec["name"] in existing_cards:
            card_id = existing_cards[spec["name"]]
            logger.info("card %r already exists, updating its query/settings", spec["name"])
            requests.put(f"{BASE}/card/{card_id}", headers=headers, json=body).raise_for_status()
            ids[spec["name"]] = card_id
            continue

        logger.info("creating card %r", spec["name"])
        resp = requests.post(f"{BASE}/card", headers=headers, json=body)
        resp.raise_for_status()
        ids[spec["name"]] = resp.json()["id"]

    return ids


def get_or_create_dashboard_id(headers: dict) -> int:
    dashboards = requests.get(f"{BASE}/dashboard", headers=headers).json()
    existing = next((d for d in dashboards if d["name"] == DASHBOARD_NAME), None)
    if existing:
        logger.info("dashboard %r already exists (id=%s)", DASHBOARD_NAME, existing["id"])
        return existing["id"]

    logger.info("creating dashboard %r", DASHBOARD_NAME)
    resp = requests.post(f"{BASE}/dashboard", headers=headers, json={"name": DASHBOARD_NAME})
    resp.raise_for_status()
    return resp.json()["id"]


def layout_dashboard(headers: dict, dash_id: int, chart_ids: dict[str, int], scalar_ids: dict[str, int]) -> None:
    """Lay out header -> KPI row -> 2x2 chart grid, replacing whatever cards
    the dashboard currently has. Re-run on every script invocation (not just
    once) so layout/styling fixes reach an already-configured instance too.
    """
    header = {
        "id": -1,
        "card_id": None,
        "row": 0,
        "col": 0,
        "size_x": 24,
        "size_y": 3,
        "visualization_settings": {
            "virtual_card": {
                "name": None,
                "display": "text",
                "visualization_settings": {},
                "dataset_query": {},
                "archived": False,
            },
            "text": HEADER_TEXT,
        },
    }

    kpi_cards = [
        {"id": -(i + 2), "card_id": cid, "row": 3, "col": i * 6, "size_x": 6, "size_y": 3}
        for i, cid in enumerate(scalar_ids.values())
    ]

    chart_positions = [(0, 0), (0, 12), (1, 0), (1, 12)]
    chart_cards = [
        {
            "id": -(i + 10),
            "card_id": cid,
            "row": 6 + chart_positions[i][0] * 9,
            "col": chart_positions[i][1],
            "size_x": 12,
            "size_y": 9,
        }
        for i, cid in enumerate(chart_ids.values())
    ]

    resp = requests.put(
        f"{BASE}/dashboard/{dash_id}/cards", headers=headers, json={"cards": [header, *kpi_cards, *chart_cards]}
    )
    resp.raise_for_status()


def main() -> int:
    if not GCP_PROJECT_ID or not CREDENTIALS_PATH:
        logger.error("GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS must be set")
        return 1

    wait_for_metabase()
    session = get_session()
    headers = {"X-Metabase-Session": session}

    db_id = get_or_create_database(headers)
    chart_ids = upsert_cards(headers, db_id, CARDS)
    scalar_ids = upsert_cards(headers, db_id, SCALAR_CARDS, display="scalar")
    dash_id = get_or_create_dashboard_id(headers)
    layout_dashboard(headers, dash_id, chart_ids, scalar_ids)

    logger.info("done: %s/dashboard/%d", os.environ.get("METABASE_URL", "http://localhost:3000"), dash_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
