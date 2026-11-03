
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select type_sport
from "warehouse"."staging"."stg_activites"
where type_sport is null



  
  
      
    ) dbt_internal_test