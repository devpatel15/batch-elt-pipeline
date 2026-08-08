
  
    

    create or replace table `batch-etl-pipeline-504804`.`dbt_prod_marts`.`dim_actor`
      
    
    

    
    OPTIONS()
    as (
      with events as (
    select actor_id, actor_login, created_at
    from `batch-etl-pipeline-504804`.`dbt_prod_intermediate`.`int_events_flattened`
    where actor_id is not null
)

select
    actor_id,
    actor_login
from events
qualify row_number() over (partition by actor_id order by created_at desc) = 1
    );
  