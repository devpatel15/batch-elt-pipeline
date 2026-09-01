# Batch ELT Pipeline - GH Archive to BigQuery

A daily batch ELT pipeline that ingests [GH Archive](https://www.gharchive.org/) events
(every public GitHub event, hourly), orchestrates the load/transform with Airflow, warehouses
in BigQuery, transforms with dbt, validates with Great Expectations, and serves a dashboard.

## Architecture

```
GH Archive (hourly .json.gz)
        │  extract_gharchive.py
        ▼
raw/YYYY/MM/DD/H.json.gz          (local landing zone)
        │  Great Expectations (schema/null/range checks)
        ▼
BigQuery raw dataset               (load job, JSONL)
        │  dbt: staging → intermediate → marts
        ▼
BigQuery marts (fct_events, dim_repo, dim_actor, agg_daily_repo_activity)
        │
        ▼
Metabase dashboard
```

Orchestrated end-to-end by Airflow (`dags/daily_elt_dag.py`), running daily.

## Status

- [x] Phase 1: Ingestion + raw landing zone
- [x] Phase 2: Airflow orchestration (extract task; load/dbt/GE tasks land in Phases 3-4)
- [x] Phase 3: BigQuery + dbt models
- [x] Phase 4: Great Expectations data quality gate
- [x] Phase 5: Metabase dashboard
- [x] Phase 6: Terraform + CI (stretch)
- [x] Phase 7: CD (dev/prod dbt targets, deploy-on-merge, published dbt docs)

## Phase 1: Ingestion

`ingestion/extract_gharchive.py` downloads hourly GH Archive files and writes them,
unmodified (still gzip-compressed JSON lines), into a partitioned raw landing zone:

```
raw/2026/08/06/0.json.gz
raw/2026/08/06/1.json.gz
...
```

**Idempotent by design**: before downloading, it HEADs the remote file and compares
`Content-Length` against any existing local file of the same hour. Matching files are
skipped, so re-running the script (e.g. from a daily Airflow task) never re-downloads
or duplicates data. A size mismatch triggers a re-download; `--force` always re-downloads.

GH Archive publishes with a lag, so by default the script only requests hours ending
2 hours before "now" to avoid spurious 404s on not-yet-published hours (still handled
gracefully if it happens).

### Usage

```bash
pip install -r requirements.txt

# Ingest the last 24 hours (default)
python ingestion/extract_gharchive.py

# Ingest a specific window
python ingestion/extract_gharchive.py --start 2026-08-01T00:00:00 --end 2026-08-02T00:00:00

# Re-download even if files already exist locally
python ingestion/extract_gharchive.py --hours 24 --force
```

### Tests

```bash
pytest tests/
```

## Phase 2: Airflow orchestration

`docker-compose.yml` runs a single-node Airflow deployment (Postgres metadata DB +
webserver + scheduler, `LocalExecutor`) with `daily_elt_dag` mounted from `dags/`.

The DAG currently has one task, `extract_gharchive`, which runs the Phase 1 script
inside the container against the run's **data interval** (not "now"), so a manual
backfill (`airflow dags backfill`) re-ingests the correct historical hours instead of
whatever happens to be recent. Load-to-BigQuery, `dbt run`/`dbt test`, and the Great
Expectations checkpoint are appended to this same DAG in Phases 3-4 as those tools
come online.

Failure alerting (`dags/alerts.py`) posts to a Slack incoming webhook read from
`SLACK_WEBHOOK_URL`; if that env var is unset, failures are logged instead of alerted
(never silently dropped, never blocks the DAG on missing Slack config).

### Usage

```bash
cp .env.example .env   # then optionally fill in SLACK_WEBHOOK_URL

docker compose up airflow-init   # one-time: migrates the metadata DB, creates the admin user
docker compose up -d             # starts postgres, webserver, scheduler

# http://localhost:8080  (login: admin / admin, from .env)
```

Trigger `daily_elt_dag` from the UI, or:

```bash
docker compose exec airflow-scheduler airflow dags trigger daily_elt_dag
```

Tear down with `docker compose down` (add `-v` to also drop the Postgres volume).

## Phase 3: BigQuery warehouse + dbt

### GCP setup (one-time, manual)

1. Create a GCP project (no billing account needed, BigQuery's free sandbox tier is enough).
2. Enable the BigQuery API for that project.
3. Create a service account with the "BigQuery Admin" role, generate a JSON key, and save it to
   `secrets/gcp-service-account.json` (gitignored, mounted read-only into the Airflow containers).
4. Set `GCP_PROJECT_ID` in `.env`.

### Loading raw data

`ingestion/load_to_bigquery.py` loads one day's already-ingested hourly files into
`raw_gharchive.events`, a table partitioned by `DATE(created_at)` and clustered by
`type`. `actor`/`repo`/`org` get an explicit RECORD schema since they're consistent
across every event type; `payload` is loaded as BigQuery's native `JSON` type since
its shape varies wildly by event type (a `PushEvent` payload looks nothing like a
`WatchEvent` payload) - dbt queries into it with `JSON_VALUE`/`JSON_QUERY` downstream.
Idempotent the same way as Phase 1: each run overwrites that day's partition
(`table$YYYYMMDD`, `WRITE_TRUNCATE`) instead of appending.

**Streams straight into the upload, never touches local disk.** `ChainedGzipReader`
decompresses each hourly file and feeds the bytes directly to BigQuery's resumable
upload, a chunk at a time. An earlier version decompressed a full day (~2.5GB) into a
temp file first; the temp file was deleted correctly afterward, but on Windows/WSL2,
Docker's dynamically-expanding virtual disk doesn't release blocks back to the host
just because the file that used them was deleted - each load permanently inflated
host disk usage by roughly the decompressed size. Streaming eliminated that at the
source: verified back-to-back loads with disk usage checked before and after -
flat, instead of dropping ~3GB per day.

**BigQuery's free sandbox tier also caps total storage at 10GB per project** - a hard
limit, hit directly while backfilling history for the dashboard (`raw_gharchive.events`
alone costs ~1.4GB/day, driven by storing the full JSON `payload` per event for
fidelity). That interacts with the full-refresh constraint above: since `fct_events`
is a full-refresh table, all raw history has to remain present for the marts to keep
reflecting it, so raw data can't just be pruned to make room without also losing that
history downstream. In practice this puts the practical retention ceiling at roughly a
week of full-fidelity raw data on the free tier - a real constraint of building on a
no-billing sandbox, not a bug.

### dbt models

`dbt_project/` follows the standard staging → intermediate → marts layering:

- **staging** (`stg_gharchive_events`): types/renames raw columns, passes `payload` through untouched.
- **intermediate** (`int_events_flattened`): flattens the type-varying JSON payload into typed
  columns (`action`, `pull_request_number`, `push_commit_count`, etc.), null wherever a column
  doesn't apply to that event type.
- **marts**: `dim_repo`, `dim_actor`, `fct_events` (partitioned/clustered fact table, one row per
  event), and `agg_daily_repo_activity` (daily per-repo rollup).

15 dbt tests (`not_null`, `unique`, `relationships`, `accepted_values`) run across the staging
and marts layers, 100% pass rate against real data.

**Why `fct_events` is a full-refresh table, not incremental**: every BigQuery incremental
strategy (`merge`, `insert_overwrite`) compiles to a DML statement, and BigQuery's free sandbox
tier, the whole point of not needing a billing account here, rejects all DML with
`"DML queries are not allowed in the free tier"`. `CREATE OR REPLACE TABLE` (a full-refresh
table materialization) is DDL, so it's the only materialization that works without billing
enabled. The table is still partitioned/clustered so downstream queries prune to just the
days/types they need.

### Usage

```bash
# one-off local load + dbt run, without Airflow
python ingestion/load_to_bigquery.py --date 2026-08-06

cd dbt_project
GCP_PROJECT_ID=<your-project> GOOGLE_APPLICATION_CREDENTIALS=../secrets/gcp-service-account.json \
  DBT_PROFILES_DIR=. dbt build
```

Inside Airflow, `daily_elt_dag` now runs `extract_gharchive` → `load_to_bigquery` → `dbt_run` →
`dbt_test` end to end (the custom `Dockerfile.airflow` image adds `dbt-core`, `dbt-bigquery`,
and `google-cloud-bigquery` on top of the stock Airflow image, rebuild it with
`docker compose build` after pulling changes to `requirements-airflow.txt`).

## Phase 4: Great Expectations data quality gate

`great_expectations/validate_raw_events.py` runs between `extract_gharchive` and
`load_to_bigquery` in the DAG. If it fails, `load_to_bigquery` (and everything after
it - dbt run, dbt test) never runs: bad data doesn't reach BigQuery at all, rather
than getting caught after the fact.

It checks, for one day's raw files:
- the expected columns are present (`id`, `type`, `actor`, `repo`, `payload`, `public`,
  `created_at` - `org` is allowed but not required, since it only appears on org-scoped events)
- the null rate on `id`/`type`/`created_at` stays under 1%
- `created_at` falls within the target day (± 6h buffer for boundary stragglers)
- `type` is one of GH Archive's known event types (the same check dbt's
  `accepted_values` test makes independently, after the load - defense in depth)

**Performance note**: at GH Archive's real volume (~2.5M events/day), an early version
of this loaded every column - including the heavy nested `payload`/`actor`/`repo`
objects - into memory for every expectation and OOM'd the container outright. The
column-presence check now runs against a 2,000-record sample instead of the full day
(safe, not just fast: GH's event envelope is fixed regardless of event type, so a
broken schema shows up in record 1 as reliably as record 2,000,000), and the
per-row threshold checks run against just the 3 lightweight columns they need,
never the heavy ones. Full-day validation dropped from OOM-killed to ~12 seconds.

**Verified it actually catches problems**, not just that it runs: a synthetic batch
with 5% null ids, 5% unknown event types, and 5% out-of-range timestamps (all above
the 1% threshold) correctly failed with exact row-level detail on every violation;
the equivalent clean batch correctly passed.

### Usage

```bash
# inside the Airflow container (needs the container's Python env - see note below)
docker compose exec airflow-scheduler python /opt/airflow/great_expectations/validate_raw_events.py \
  --date 2026-08-06 --raw-dir /opt/airflow/raw
```

> **Why this only runs in Docker, not the local venv**: Great Expectations pulls in
> pandas/numpy, and this project's local Windows venv is on Python 3.14 - new enough
> that numpy has no official Windows wheel for it yet, so pip falls back to an
> experimental, `CRASHES ARE TO BE EXPECTED`-labeled MinGW build that segfaults on
> import. The Airflow container runs a stable Linux Python with proper wheels, which
> is also the pipeline's real execution environment anyway, so GE-dependent code is
> developed and tested there rather than fighting the local interpreter.

## Phase 5: Metabase dashboard

`docker-compose.yml` runs Metabase (its own Postgres app database, separate from Airflow's)
on `localhost:3000`, scoped to the `dbt_dev_marts` BigQuery dataset only - it never sees the
raw or staging layers.

`scripts/setup_metabase.py` automates the entire setup via Metabase's REST API instead of the
manual browser wizard: creates the admin account, connects BigQuery, builds four charts as
native SQL questions, and assembles them into a "GH Archive Activity" dashboard. It's
idempotent - re-running it against an already-configured instance detects the existing admin
session, database, cards, and dashboard by name and touches nothing, rather than duplicating
them (verified: ran it twice, second run recognized everything, `/api/card` and `/api/dashboard`
still showed exactly one of each afterward).

Charts, all querying the marts layer directly:
- **Daily Event Volume** (line) - total events per day from `agg_daily_repo_activity`
- **Top 10 Repos by Activity** (bar) - `agg_daily_repo_activity` joined to `dim_repo`
- **Most Active Hours (UTC)** (bar) - event counts by hour from `fct_events`
- **Event Type Breakdown** (pie) - event counts by type from `fct_events`

### Usage

```bash
docker compose up -d metabase-db metabase
python scripts/setup_metabase.py   # needs GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS,
                                    # METABASE_ADMIN_EMAIL/PASSWORD from .env in the environment
```

Then open `http://localhost:3000` and log in with `METABASE_ADMIN_EMAIL`/`METABASE_ADMIN_PASSWORD`.

## Phase 6: Terraform + CI (stretch)

### Terraform

`terraform/` provisions only the **raw** landing-zone dataset/table
(`raw_gharchive.events`) - schema, DAY partitioning on `created_at`, clustering on
`type`, matching `ingestion/load_to_bigquery.py`'s `TABLE_SCHEMA` field for field.
The dbt-managed datasets (`dbt_dev_staging`/`intermediate`/`marts`) are deliberately
**not** in Terraform: dbt creates and owns those itself on every run, so managing them
in two places would just invite drift. Terraform owns the foundational raw layer; dbt
owns everything built on top of it.

**Verified against real infrastructure, not just written**: imported the actual
`raw_gharchive` dataset/table (the same ones every other phase in this README has been
using) into Terraform state and ran `terraform plan` - schema, partitioning, and
clustering all matched with zero diff on the first try. The only differences were two
worth keeping deliberately: BigQuery's sandbox tier auto-applies a 60-day dataset
expiration that the initial config hadn't declared (now explicit, so `terraform apply`
doesn't accidentally strip it), and `deletion_protection`, which is intentionally
`false` for disposable landing-zone data. After `terraform apply`, a follow-up
`terraform plan` reports **"No changes. Your infrastructure matches the
configuration."**

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your project_id
terraform init
terraform plan
terraform apply
```

### GitHub Actions CI

`.github/workflows/ci.yml` runs on every PR (deploying to prod is a separate concern,
see Phase 7):

- **`test`** (no secrets needed, runs on every PR including forks): `pytest tests/`,
  plus the Great Expectations gate run for real against a small committed fixture
  (`tests/fixtures/raw/`) instead of a full day of real GH Archive data - the gate
  needs no BigQuery credentials at all (it only ever reads local files), so this is a
  genuine exercise of the same code path Airflow runs, just against fixture data
  instead of a live day, keeping CI fast and deterministic.
- **`dbt`** (needs repo secrets, so it doesn't run on forked-repo PRs - GitHub
  withholds secrets from those for security; PR-only, not on push - see Phase 7 for
  why): runs `dbt build` against the `dev` target.

To enable the `dbt` job, add these repository secrets (Settings → Secrets and
variables → Actions):
- `GCP_PROJECT_ID` - your GCP project ID
- `GCP_SERVICE_ACCOUNT_KEY` - the full contents of `secrets/gcp-service-account.json`

## Phase 7: CD

Merging to `master` automatically applies infrastructure changes, promotes dbt models
to a `prod` dataset separate from what PRs build against, and publishes a docs site,
via `.github/workflows/cd.yml`.

### Remote Terraform state: Terraform Cloud, not GCS

The original plan for this phase was a GCS bucket as the Terraform state backend, so
state persists across CI runs instead of living only on one local machine. That's
blocked on this project specifically: creating *any* Cloud Storage bucket requires an
active GCP billing account, unlike BigQuery, which has a billing-free sandbox mode.
Since this project deliberately runs without a billing account (the same reason
`fct_events` is full-refresh, not incremental - see Phase 3), a GCS bucket was never
an option here.

Terraform Cloud's free tier gives the same result (state reachable from CI, not
local-only) without that requirement:

```hcl
# terraform/versions.tf
terraform {
  cloud {
    organization = "your-org-name"
    workspaces {
      name = "batch-elt-pipeline"
    }
  }
}
```

One-time setup:
1. Create a free account at [app.terraform.io](https://app.terraform.io) and an organization.
2. Create a user API token (User Settings → Tokens).
3. Run `terraform init` locally with `TF_TOKEN_app_terraform_io=<token>` set - this
   creates the workspace (if it doesn't exist yet) and offers to migrate existing local
   state into it.
4. Set the workspace's execution mode to **Local** (Workspace Settings → General).
   Terraform Cloud workspaces default to *remote* execution, where `plan`/`apply`
   literally run on Terraform Cloud's own infrastructure rather than wherever you
   invoked `terraform` - which breaks `credentials = file(var.credentials_file)` in
   `versions.tf`, since the service account key isn't part of what gets uploaded there.
   Local execution mode makes Terraform Cloud purely a state backend, matching what a
   "backend" conceptually should do: `plan`/`apply` still run in GitHub Actions (or
   locally), Terraform Cloud just stores the state.
5. Add `TF_TOKEN_app_terraform_io` as a GitHub repository secret (same token from step 2).

### dev/prod dbt targets: a materialization split, not a dataset lifecycle

`dbt_project/profiles.yml` has a `prod` target alongside `dev`, pointing at
`dbt_prod_*` datasets instead of `dbt_dev_*`. `dev` stays the default for PRs and local
work; `prod` is only ever invoked explicitly (`--target prod`).

This exists for a real reason, not just as a resume checkbox: before this phase, the
`dbt` CI job ran `dbt build` against `dbt_dev_marts` on every PR, and that was the
exact dataset Metabase's live dashboard read from - a bad PR could already overwrite
production data before merge. `scripts/setup_metabase.py` now points at
`dbt_prod_marts` instead, so PR-triggered dev rebuilds can never touch what the
dashboard shows.

The isolation itself comes from having separate datasets at all, not from how they're
materialized. Materialization is a separate, purely cost-driven decision: this
project's BigQuery usage sits at 8.5GB of the free sandbox tier's 10GB storage cap
(checked live while building this phase), and `dbt_dev_marts` alone was 1.53GB of
that. A second permanent `dbt_prod_marts` copy at the same size would have pushed
past 10GB with zero room for `raw_gharchive` to keep growing. BigQuery views cost
no storage at all (just a saved query, computed at read time), so marts materializes
as a `view` on `dev` and a `table` on `prod`, conditioned on `target.name`:

```yaml
# dbt_project.yml
marts:
  +materialized: "{{ 'table' if target.name == 'prod' else 'view' }}"
```

`fct_events.sql` has its own `config()` block (for `partition_by`/`cluster_by`, which
have no view equivalent) that overrides this project-level default, so it needs the
same conditional repeated in the model itself - easy to miss, since the model-level
override silently wins if only the project default gets changed. At ~1.25GB of marts'
~1.5GB, it's also the model where getting this right actually matters.

**Known tradeoff, not a blocker at current scale**: staging and intermediate are
already views, so `dev`'s `fct_events` is now a view stacked on views, over a 7GB+
`raw_gharchive` table. Each of the 4 dbt tests that touch it reprocesses that whole
chain from raw independently, roughly 35-100GB of query processing per dev
`dbt build`, against BigQuery's separate 1TB/month query quota (not the 10GB storage
one). Fine for this project's PR cadence; would be worth revisiting with significantly
higher data volume or CI frequency.

### The CD job

`.github/workflows/cd.yml` triggers only on push to `master` (never PRs, since it
writes to real infrastructure and prod), and:
1. Runs `terraform apply` against `terraform/` (the raw dataset/table - almost always
   a no-op in practice, since that infrastructure rarely changes; still runs every
   time so drift gets caught immediately if it ever does).
2. Runs `dbt run --target prod` and `dbt test --target prod`.
3. Runs `dbt docs generate --target prod` and publishes the static site to the
   `gh-pages` branch via `peaceiris/actions-gh-pages`. Once GitHub Pages is enabled
   for the repo (Settings → Pages → Source: `gh-pages` branch, a one-time manual step),
   the docs are browsable at `https://<username>.github.io/<repo>/`.

The `GCP_SERVICE_ACCOUNT_KEY` secret already had the "BigQuery Admin" role from Phase
3, which already covers everything `terraform apply` needs to modify BigQuery
datasets/tables - no change needed there. It separately needed "Storage Admin" added
during Phase 7 while GCS was still the plan; since the state backend ended up being
Terraform Cloud instead, that role isn't actually used by anything in this project
and can be safely removed again.

## Requirements

- Python 3.11+
- Docker + Docker Compose (Phase 2+)
- A GCP project with BigQuery API enabled (Phase 3+), free sandbox tier, no credit card required
