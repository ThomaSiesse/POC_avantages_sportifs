
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select motif_rejet
from "dwh"."monitoring"."mon_rejets_activites"
where motif_rejet is null



  
  
      
    ) dbt_internal_test