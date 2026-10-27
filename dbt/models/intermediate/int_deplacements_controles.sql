-- Contrôle des déclarations de déplacement domicile-travail sportif (marche, vélo...)
-- statut : 'ok', 'incoherent' (distance > seuil) ou 'non_calcule' (adresse introuvable,
-- API indisponible). Les seuils sont dans seeds/regles_deplacement.csv.
with rh as (
    select * from {{ ref('stg_rh') }}
),

regles as (
    select * from {{ ref('regles_deplacement') }}
),

distances as (
    select * from {{ ref('int_distances_domicile') }}
)

select
    rh.id_salarie,
    rh.prenom,
    rh.nom,
    rh.adresse_domicile,
    rh.moyen_deplacement,
    distances.distance_km,
    regles.distance_max_km,
    case
        when distances.distance_km is null then 'non_calcule'
        when distances.distance_km > regles.distance_max_km then 'incoherent'
        else 'ok'
    end as statut
from rh
join regles using (moyen_deplacement)
-- la jointure sur l'adresse et le mode ignore les calculs devenus obsolètes
left join distances using (id_salarie, adresse_domicile, moyen_deplacement)
