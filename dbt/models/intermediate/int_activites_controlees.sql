-- Toutes les activités, avec leurs indicateurs calculés et la raison d'un éventuel rejet
-- (motif_rejet NULL = activité valide). Les seuils sont dans seeds/regles_sports.csv.
with activites as (
    select * from {{ ref('stg_activites') }}
),

rh as (
    select id_salarie from {{ ref('stg_rh') }}
),

regles as (
    select * from {{ ref('regles_sports') }}
),

calculs as (
    select
        activites.*,
        date_diff('second', date_debut, date_fin) / 60.0 as duree_min,
        case
            when distance_m is not null and date_fin > date_debut
                then (distance_m / 1000.0) / (date_diff('second', date_debut, date_fin) / 3600.0)
        end as vitesse_kmh,
        regles.vitesse_max_kmh,
        rh.id_salarie is not null as salarie_connu
    from activites
    left join regles using (type_sport)
    left join rh using (id_salarie)
)

select
    *,
    case
        when not salarie_connu then 'salarié inconnu des données RH'
        -- Date valide
        when date_fin <= date_debut then 'date de fin antérieure à la date de début'
        when duree_min > 12 * 60 then 'durée supérieure à 12 h'
        when date_debut > now() then 'date de début dans le futur'
        when date_fin > now() then 'date de fin dans le futur'
        when date_debut < now() - interval 1 year then 'activité de plus d''un an'
        -- Distance positive et réaliste
        when distance_m <= 0 then 'distance négative ou nulle'
        when vitesse_kmh > vitesse_max_kmh then 'vitesse irréaliste pour le sport'
    end as motif_rejet
from calculs
