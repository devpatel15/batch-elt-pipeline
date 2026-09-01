with events as (
    select actor_id, actor_login, created_at
    from {{ ref('int_events_flattened') }}
    where actor_id is not null
)

select
    actor_id,
    actor_login
from events
qualify row_number() over (partition by actor_id order by created_at desc) = 1
