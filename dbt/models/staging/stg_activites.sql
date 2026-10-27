-- Une ligne par activité : dernière version connue de chaque id_activite
-- (le raw contient le snapshot Debezium, les créations et d'éventuels doublons)
with source as (
    select * from {{ source('minio_raw', 'activites') }}
),

derniere_version as (
    select *
    from source
    qualify row_number() over (
        partition by id_activite
        order by source_ts_ms desc, ingested_at desc
    ) = 1
)

select
    id_activite,
    id_salarie,
    cast(date_debut as timestamptz) as date_debut,
    cast(date_fin as timestamptz) as date_fin,
    type_sport,
    distance_m,
    commentaire,
    op as derniere_operation,
    ingested_at
from derniere_version
