
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select repo_id
from `batch-etl-pipeline-504804`.`dbt_prod_marts`.`dim_repo`
where repo_id is null



  
  
      
    ) dbt_internal_test