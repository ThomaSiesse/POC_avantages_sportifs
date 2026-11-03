
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_salarie
from "dwh"."marts"."mart_avantages_salaries"
where id_salarie is null



  
  
      
    ) dbt_internal_test