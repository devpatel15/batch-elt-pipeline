# Provisions the raw landing-zone dataset/table that
# ingestion/load_to_bigquery.py loads into. The dbt-managed datasets
# (dbt_dev_staging/intermediate/marts) are intentionally NOT provisioned here:
# dbt creates and owns those itself on every run, so managing them in two
# places would just invite drift. Terraform owns the foundational raw layer;
# dbt owns everything built on top of it.
#
# Schema here is kept in sync by hand with ingestion/load_to_bigquery.py's
# TABLE_SCHEMA - if the row shape changes there, mirror the field list here
# and re-run `terraform plan` to check for drift before `apply`.

resource "google_bigquery_dataset" "raw_gharchive" {
  dataset_id  = "raw_gharchive"
  project     = var.project_id
  location    = var.location
  description = "Raw GH Archive events landing zone, loaded by ingestion/load_to_bigquery.py"

  # BigQuery's free sandbox tier applies a 60-day default expiration to
  # datasets/tables it creates, to bound storage on an account with no
  # billing attached. Declared explicitly here (rather than left unset, which
  # would have Terraform strip it) since it's a sensible default worth
  # keeping for disposable landing-zone data, not an accident to undo.
  default_partition_expiration_ms = 5184000000
  default_table_expiration_ms     = 5184000000
}

resource "google_bigquery_table" "events" {
  dataset_id = google_bigquery_dataset.raw_gharchive.dataset_id
  table_id   = "events"
  project    = var.project_id

  # BigQuery's free sandbox tier (no billing account) doesn't allow deletion
  # protection to be paired with certain lifecycle operations cleanly in
  # every case, and this is disposable landing-zone data reloadable from the
  # raw/ files on disk - false is the right default for a portfolio project.
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["type"]

  schema = jsonencode([
    { name = "id", type = "STRING" },
    { name = "type", type = "STRING" },
    {
      name = "actor",
      type = "RECORD",
      fields = [
        { name = "id", type = "INT64" },
        { name = "login", type = "STRING" },
        { name = "display_login", type = "STRING" },
        { name = "gravatar_id", type = "STRING" },
        { name = "url", type = "STRING" },
        { name = "avatar_url", type = "STRING" },
      ]
    },
    {
      name = "repo",
      type = "RECORD",
      fields = [
        { name = "id", type = "INT64" },
        { name = "name", type = "STRING" },
        { name = "url", type = "STRING" },
      ]
    },
    {
      name = "org",
      type = "RECORD",
      fields = [
        { name = "id", type = "INT64" },
        { name = "login", type = "STRING" },
        { name = "gravatar_id", type = "STRING" },
        { name = "url", type = "STRING" },
        { name = "avatar_url", type = "STRING" },
      ]
    },
    { name = "payload", type = "JSON" },
    { name = "public", type = "BOOL" },
    { name = "created_at", type = "TIMESTAMP" },
  ])
}
