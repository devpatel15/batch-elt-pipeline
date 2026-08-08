
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select actor_id
from `batch-etl-pipeline-504804`.`dbt_prod_marts`.`dim_actor`
where actor_id is null



  
  
      
    ) dbt_internal_test