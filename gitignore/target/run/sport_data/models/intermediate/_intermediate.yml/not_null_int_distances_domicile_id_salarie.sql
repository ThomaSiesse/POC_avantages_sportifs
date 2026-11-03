
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_salarie
from "warehouse"."intermediate"."int_distances_domicile"
where id_salarie is null



  
  
      
    ) dbt_internal_test