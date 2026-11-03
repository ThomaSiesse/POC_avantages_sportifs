
    
    

select
    id_activite as unique_field,
    count(*) as n_records

from "warehouse"."staging"."stg_activites"
where id_activite is not null
group by id_activite
having count(*) > 1


