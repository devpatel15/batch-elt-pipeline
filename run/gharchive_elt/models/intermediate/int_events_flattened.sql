

  create or replace view `batch-etl-pipeline-504804`.`dbt_prod_intermediate`.`int_events_flattened`
  OPTIONS()
  as -- Flattens the type-varying `payload` JSON into typed columns. Each event
-- type populates a different subset; columns are null wherever they don't
-- apply (e.g. push_commit_count is null for anything but PushEvent).

with staged as (
    select * from `batch-etl-pipeline-504804`.`dbt_prod_staging`.`stg_gharchive_events`
)

select
    event_id,
    event_type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_id,
    org_login,
    is_public,
    created_at,

    -- present on most state-change event types
    json_value(payload, '$.action') as action,

    -- CreateEvent / DeleteEvent
    json_value(payload, '$.ref') as ref,
    json_value(payload, '$.ref_type') as ref_type,

    -- PushEvent
    safe_cast(json_value(payload, '$.distinct_size') as int64) as push_commit_count,

    -- PullRequestEvent
    safe_cast(json_value(payload, '$.number') as int64) as pull_request_number,

    -- IssuesEvent / IssueCommentEvent
    safe_cast(json_value(payload, '$.issue.number') as int64) as issue_number

from staged;

