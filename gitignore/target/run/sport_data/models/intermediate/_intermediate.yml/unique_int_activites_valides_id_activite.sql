
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    id_activite as unique_field,
    count(*) as n_records

from "warehouse"."intermediate"."int_activites_valides"
where id_activite is not null
group by id_activite
having count(*) > 1



  
  
      
    ) dbt_internal_test