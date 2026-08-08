-- Full-refresh table in prod, not incremental, by necessity: every
-- incremental strategy BigQuery supports (merge, insert_overwrite) compiles
-- to a DML statement (MERGE/INSERT/DELETE), and BigQuery's free sandbox tier
-- (the whole point of using BigQuery for this project without a billing
-- account) rejects all DML with "DML queries are not allowed in the free
-- tier". CREATE OR REPLACE TABLE (what a full-refresh table materialization
-- does) is DDL, not DML, so it's the only materialization that works here.
--
-- In dev it's a view instead: this model has its own config() block, which
-- overrides the project-level default in dbt_project.yml, so the
-- table/view-by-target split has to be repeated here too. dev is a view for
-- the same reason as the rest of marts (BigQuery views cost no storage), and
-- this is the one that matters most for that: at ~1.25GB, fct_events alone
-- is most of marts' storage footprint.
--
-- partition_by/cluster_by are BigQuery table options with no view
-- equivalent; dbt-bigquery just ignores them when the resolved
-- materialization isn't 'table', so they're left unconditional.


select
    event_id,
    event_type,
    actor_id,
    repo_id,
    org_id,
    is_public,
    action,
    ref,
    ref_type,
    push_commit_count,
    pull_request_number,
    issue_number,
    created_at
from `batch-etl-pipeline-504804`.`dbt_prod_intermediate`.`int_events_flattened`