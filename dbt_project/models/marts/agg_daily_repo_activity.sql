-- A small fraction of events (observed: some ForkEvents) carry a null repo
-- in GH Archive's own source data - the real repo lives only in the nested
-- payload, not the top-level `repo` field. dim_repo already excludes these;
-- this being a *per-repo* rollup, a row with no repo doesn't belong in it
-- either, so they're excluded here rather than the aggregate having to grow
-- an "unattributed activity" row.
select
    date(created_at) as activity_date,
    repo_id,
    count(*) as total_events,
    countif(event_type = 'PushEvent') as push_events,
    countif(event_type = 'WatchEvent') as star_events,
    countif(event_type = 'ForkEvent') as fork_events,
    countif(event_type = 'PullRequestEvent') as pull_request_events,
    countif(event_type = 'IssuesEvent') as issue_events,
    count(distinct actor_id) as distinct_actors
from {{ ref('fct_events') }}
where repo_id is not null
group by 1, 2
