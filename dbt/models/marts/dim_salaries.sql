-- Une ligne par salarié : informations RH utiles à l'analyse (sans adresse ni date de naissance)
with rh as (
    select * from {{ ref('stg_rh') }}
),

sportives as (
    select * from {{ ref('stg_sportives') }}
),

deplacements as (
    select * from {{ ref('int_deplacements_controles') }}
)

select
    rh.id_salarie,
    rh.prenom,
    rh.nom,
    rh.bu,
    rh.type_contrat,
    rh.date_embauche,
    rh.salaire_brut,
    rh.moyen_deplacement,
    deplacements.id_salarie is not null as deplacement_sportif,
    deplacements.distance_km as distance_domicile_km,
    deplacements.statut as statut_controle_deplacement,
    sportives.sport_pratique,
    sportives.sport_pratique is not null as pratique_sport
from rh
left join sportives using (id_salarie)
left join deplacements using (id_salarie)
