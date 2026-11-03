
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select id_salarie as from_field
    from "dwh"."marts"."fct_activites"
    where id_salarie is not null
),

parent as (
    select id_salarie as to_field
    from "dwh"."marts"."dim_salaries"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test