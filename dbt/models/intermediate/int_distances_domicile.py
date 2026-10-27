"""
Distance domicile -> entreprise, par la route, selon le mode de déplacement déclaré.

Calculée avec Google Maps via SerpApi (moteur google_maps_directions, itinéraire à
pied ou à vélo), uniquement pour les modes concernés par la prime
(seeds/regles_deplacement.csv).

Modèle incrémental : l'API n'est appelée que pour les couples
(salarié, adresse, mode de déplacement) pas encore calculés. Un salarié qui
déménage ou change de mode est donc recalculé automatiquement (1 crédit SerpApi par appel).
"""

import os
from datetime import UTC, datetime

import pyarrow as pa
import requests

SERPAPI_URL = "https://serpapi.com/search.json"

SCHEMA = pa.schema(
    [
        ("id_salarie", pa.int32()),
        ("adresse_domicile", pa.string()),
        ("moyen_deplacement", pa.string()),
        ("distance_km", pa.float64()),
        ("calcule_le", pa.timestamp("us", tz="UTC")),
    ]
)


def distance_km(domicile: str, entreprise: str, travel_mode: int, api_key: str) -> float | None:
    """Distance du plus court itinéraire Google Maps (travel_mode : 1 vélo, 2 marche).

    None si Google ne trouve pas d'itinéraire (adresse introuvable)."""
    response = requests.get(
        SERPAPI_URL,
        params={
            "engine": "google_maps_directions",
            "api_key": api_key,
            "start_addr": domicile,
            "end_addr": entreprise,
            "travel_mode": travel_mode,
            "hl": "fr",
            "gl": "fr",
        },
        timeout=30,
    )
    response.raise_for_status()
    itineraires = response.json().get("directions")
    if not itineraires:
        return None
    return min(itineraire["distance"] for itineraire in itineraires) / 1000


def model(dbt, session):
    dbt.config(materialized="incremental")

    # dbt.ref() renvoie une relation DuckDB : on l'expose en vue pour l'utiliser en SQL
    dbt.ref("stg_rh").create_view("rh")
    dbt.ref("regles_deplacement").create_view("regles")
    deja_calcules = (
        f"anti join {dbt.this} using (id_salarie, adresse_domicile, moyen_deplacement)"
        if dbt.is_incremental
        else ""
    )
    salaries = session.sql(f"""
        select rh.id_salarie, rh.adresse_domicile, rh.moyen_deplacement, regles.travel_mode
        from rh
        join regles using (moyen_deplacement)
        {deja_calcules}
    """).fetchall()

    api_key = os.getenv("SERPAPI_API_KEY", "").strip()
    if not api_key:
        print(f"SERPAPI_API_KEY absente : {len(salaries)} distances non calculées")
        return pa.Table.from_pylist([], schema=SCHEMA)

    entreprise = dbt.config.get("adresse_entreprise")
    rows = []
    for id_salarie, adresse, moyen, travel_mode in salaries:
        try:
            km = distance_km(adresse, entreprise, travel_mode, api_key)
        except (requests.RequestException, KeyError, ValueError) as e:
            # Erreur réseau / API : non enregistré, sera retenté au prochain dbt build
            print(f"Distance non calculée pour le salarié {id_salarie} ({adresse}) : {e}")
            continue
        if km is None:
            # Mis en cache avec une distance vide pour ne pas repayer l'appel à chaque build
            print(f"Adresse introuvable pour le salarié {id_salarie} : {adresse}")
        rows.append(
            {
                "id_salarie": id_salarie,
                "adresse_domicile": adresse,
                "moyen_deplacement": moyen,
                "distance_km": round(km, 2) if km is not None else None,
                "calcule_le": datetime.now(UTC),
            }
        )
    print(f"{len(rows)} distances calculées sur {len(salaries)}")
    return pa.Table.from_pylist(rows, schema=SCHEMA)
