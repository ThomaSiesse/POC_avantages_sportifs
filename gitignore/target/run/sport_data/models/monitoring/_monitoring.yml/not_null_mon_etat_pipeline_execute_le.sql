
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select execute_le
from "dwh"."monitoring"."mon_etat_pipeline"
where execute_le is null



  
  
      
    ) dbt_internal_test