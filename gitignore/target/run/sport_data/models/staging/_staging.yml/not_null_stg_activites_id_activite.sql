
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_activite
from "warehouse"."staging"."stg_activites"
where id_activite is null



  
  
      
    ) dbt_internal_test