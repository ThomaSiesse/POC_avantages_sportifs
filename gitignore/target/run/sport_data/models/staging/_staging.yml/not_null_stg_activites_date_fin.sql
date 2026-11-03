
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_fin
from "warehouse"."staging"."stg_activites"
where date_fin is null



  
  
      
    ) dbt_internal_test