
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        statut as value_field,
        count(*) as n_records

    from "warehouse"."intermediate"."int_deplacements_controles"
    group by statut

)

select *
from all_values
where value_field not in (
    'ok','incoherent','non_calcule'
)



  
  
      
    ) dbt_internal_test