-- Une ligne ajoutée à chaque dbt build : volumes par couche et fraîcheur des données
-- (incrémental sans clé = ajout simple, l'historique sert aux courbes Grafana)
with raw as (
    select count(*) as nb_lignes_raw, max(ingested_at) as derniere_ingestion_minio
    from {{ source('minio_raw', 'activites') }}
),

activites as (
    select
        count(*) as nb_activites,
        count(*) filter (where motif_rejet is null) as nb_activites_valides,
        count(*) filter (where motif_rejet is not null) as nb_activites_rejetees,
        max(date_fin) as derniere_activite
    from {{ ref('int_activites_controlees') }}
),

avantages as (
    select
        count(*) filter (where eligible_prime) as nb_eligibles_prime,
        count(*) filter (where eligible_bien_etre) as nb_eligibles_bien_etre,
        sum(montant_prime) + sum(cout_jours_bien_etre) as cout_total_avantages
    from {{ ref('mart_avantages_salaries') }}
),

deplacements as (
    select
        count(*) filter (where statut = 'incoherent') as nb_deplacements_incoherents,
        count(*) filter (where statut = 'non_calcule') as nb_deplacements_non_calcules
    from {{ ref('int_deplacements_controles') }}
)

select
    now() as execute_le,
    raw.*,
    activites.*,
    avantages.*,
    deplacements.*
from raw, activites, avantages, deplacements
