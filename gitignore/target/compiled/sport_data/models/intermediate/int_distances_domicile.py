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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"regles_deplacement": "\"warehouse\".\"seeds\".\"regles_deplacement\"", "stg_rh": "\"warehouse\".\"staging\".\"stg_rh\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {'adresse_entreprise': '1362 Av. des Platanes, 34970 Lattes'}
meta_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "warehouse"
    schema = "intermediate"
    identifier = "int_distances_domicile"
    
    def __repr__(self):
        return '"warehouse"."intermediate"."int_distances_domicile"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = True

# COMMAND ----------


