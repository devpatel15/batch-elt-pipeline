with source as (
    select * from `batch-etl-pipeline-504804`.`raw_gharchive`.`events`
)

select
    id as event_id,
    type as event_type,
    actor.id as actor_id,
    nullif(actor.login, '') as actor_login,
    repo.id as repo_id,
    repo.name as repo_name,
    org.id as org_id,
    nullif(org.login, '') as org_login,
    payload,
    public as is_public,
    created_at
from source
where id is not null
  and created_at is not null