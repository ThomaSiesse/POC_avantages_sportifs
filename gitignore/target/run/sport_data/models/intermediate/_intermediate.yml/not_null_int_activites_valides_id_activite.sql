
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_activite
from "warehouse"."intermediate"."int_activites_valides"
where id_activite is null



  
  
      
    ) dbt_internal_test