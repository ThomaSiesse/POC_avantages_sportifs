-- Nombre d'activités rejetées par motif, ajouté à chaque dbt build
select
    now() as execute_le,
    motif_rejet,
    count(*) as nb_activites
from {{ ref('int_activites_controlees') }}
where motif_rejet is not null
group by motif_rejet
