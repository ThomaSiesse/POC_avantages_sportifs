
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_debut
from "warehouse"."staging"."stg_activites"
where date_debut is null



  
  
      
    ) dbt_internal_test