-- type_sport est NULL pour les salariés qui ne déclarent pas de sport
select
    cast("ID salarié" as integer) as id_salarie,
    "Pratique d'un sport" as sport_pratique
from {{ source('excel', 'donnees_sportive') }}
