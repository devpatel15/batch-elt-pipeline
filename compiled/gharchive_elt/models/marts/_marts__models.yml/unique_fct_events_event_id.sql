
    
    

with dbt_test__target as (

  select event_id as unique_field
  from `batch-etl-pipeline-504804`.`dbt_prod_marts`.`fct_events`
  where event_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


