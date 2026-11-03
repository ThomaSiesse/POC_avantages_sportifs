
    
    

select
    id_salarie as unique_field,
    count(*) as n_records

from "warehouse"."staging"."stg_rh"
where id_salarie is not null
group by id_salarie
having count(*) > 1


