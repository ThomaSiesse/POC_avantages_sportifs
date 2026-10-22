-- Activités générées par generator.py
-- (les données RH et sportives restent dans les Excel et sont intégrées côté dbt)
CREATE TABLE IF NOT EXISTS activites (
    id_activite         BIGSERIAL PRIMARY KEY,
    id_salarie          INTEGER NOT NULL,
    date_debut          TIMESTAMPTZ NOT NULL,
    type_sport          TEXT NOT NULL,
    distance_m          INTEGER,          -- NULL pour les sports sans distance (escalade…)
    date_fin            TIMESTAMPTZ NOT NULL,
    commentaire         TEXT
);
