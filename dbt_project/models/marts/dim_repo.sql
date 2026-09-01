with events as (
    select repo_id, repo_name, created_at
    from {{ ref('int_events_flattened') }}
    where repo_id is not null
)

select
    repo_id,
    repo_name
from events
qualify row_number() over (partition by repo_id order by created_at desc) = 1
