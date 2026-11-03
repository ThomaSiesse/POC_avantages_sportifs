
  
  create view "warehouse"."staging"."stg_sportives__dbt_tmp" as (
    -- type_sport est NULL pour les salariés qui ne déclarent pas de sport
select
    cast("ID salarié" as integer) as id_salarie,
    "Pratique d'un sport" as sport_pratique
from read_xlsx('/data/donnees_sportive.xlsx', all_varchar = true)
  );
