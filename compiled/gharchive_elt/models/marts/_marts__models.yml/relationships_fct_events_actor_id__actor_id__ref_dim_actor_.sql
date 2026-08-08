
    
    

with child as (
    select actor_id as from_field
    from `batch-etl-pipeline-504804`.`dbt_prod_marts`.`fct_events`
    where actor_id is not null
),

parent as (
    select actor_id as to_field
    from `batch-etl-pipeline-504804`.`dbt_prod_marts`.`dim_actor`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


