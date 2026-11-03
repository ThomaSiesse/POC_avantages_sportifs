
    
    

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


