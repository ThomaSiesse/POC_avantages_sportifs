-- Une ligne par activité valide (12 derniers mois), prête pour les visuels Power BI
select
    id_activite,
    id_salarie,
    type_sport,
    date_debut,
    date_fin,
    cast(date_debut as date) as date_activite,
    date_trunc('month', date_debut)::date as mois_activite,
    round(distance_m / 1000.0, 2) as distance_km,
    round(duree_min, 1) as duree_min,
    round(vitesse_kmh, 1) as vitesse_kmh,
    commentaire
from {{ ref('int_activites_valides') }}
