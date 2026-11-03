
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select distance_km
from "warehouse"."main_intermediate"."int_distances_domicile"
where distance_km is null



  
  
      
    ) dbt_internal_test