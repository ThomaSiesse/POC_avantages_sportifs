
    
    

select
    id_salarie as unique_field,
    count(*) as n_records

from "dwh"."marts"."mart_avantages_salaries"
where id_salarie is not null
group by id_salarie
having count(*) > 1


