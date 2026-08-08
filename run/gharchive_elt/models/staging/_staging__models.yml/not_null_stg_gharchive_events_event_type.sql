
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_type
from `batch-etl-pipeline-504804`.`dbt_prod_staging`.`stg_gharchive_events`
where event_type is null



  
  
      
    ) dbt_internal_test