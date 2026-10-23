"""
Générateur d'activités sportives (format inspiré de l'API Strava).

1. Lit les sports déclarés par les salariés (donnees_sportive.xlsx).
2. Si la table activites est vide, génère un historique sur les 12 derniers mois.
3. Puis insère une nouvelle activité toutes les GENERATION_INTERVAL_S secondes
   et supprime chaque jour les activités de plus d'un an.

Les activités sont écrites dans Postgres : c'est Debezium qui les publie ensuite dans Kafka.
"""

import logging
import os
import random
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("generator")

# --- Configuration ---------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "../data"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "365"))
MIN_ACTIVITIES_PER_YEAR = int(os.getenv("MIN_ACTIVITIES_PER_YEAR", "5"))
MAX_ACTIVITIES_PER_YEAR = int(os.getenv("MAX_ACTIVITIES_PER_YEAR", "40"))
GENERATION_INTERVAL_S = int(os.getenv("GENERATION_INTERVAL_S", "60"))
# Part d'activités volontairement incohérentes, pour tester le filtrage Slack / dbt
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.02"))
TZ = ZoneInfo("Europe/Paris")

SPORT_COLUMNS = {"ID salarié": "id_salarie", "Pratique d'un sport": "sport"}

# Sports avec distance : (distance min km, distance max km, vitesse min km/h, vitesse max km/h)
DISTANCE_SPORTS = {
    "Runing": (1, 100, 4, 22),
    "Randonnée": (5, 50, 1.5, 7.5),
    "Triathlon": (5, 100, 8, 50),
    "Natation": (0.5, 10, 1, 6),
    "Voile": (2, 40, 4, 40),
    "Équitation": (1, 30, 4, 15),
    "Tennis": (0.2, 10, 0.1, 6),
    "Badminton": (0.2, 10, 0.1, 6),
    "Tennis de table": (0.2, 10, 0.1, 6),
    "Football": (1, 15, 1, 10),
    "Basketball": (1, 100, 1, 10),
    "Rugby": (1, 15, 1, 10),
}
# Sports sans distance : (durée min minutes, durée max minutes)
DURATION_SPORTS = {
    "Escalade": (30, 180),
    "Tennis de table": (30, 90),
    "Judo": (30, 120),
    "Boxe": (30, 90),
}
DEFAULT_DURATION = (30, 90)

COMMENTAIRES = [
    "Super séance !",
    "Reprise en douceur",
    "Bonnes sensations aujourd'hui",
    "Un peu fatigué(e)",
    "Nouveau record personnel 🎉",
    "Séance avec les collègues",
    "Météo difficile mais motivé(e)",
    "Objectif atteint",
]


# --- Base de données ---------------------------------------------------------
def connect(retries: int = 10, delay_s: int = 3) -> psycopg.Connection:
    """Connexion à postgres-source, avec quelques tentatives au démarrage."""
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "5435")),
                user=os.getenv("DB_USER", "app"),
                password=os.getenv("DB_PASSWORD", "app"),
                dbname=os.getenv("DB_NAME", "sport"),
            )
        except psycopg.OperationalError:
            logger.warning("Postgres indisponible (tentative %d/%d)", attempt, retries)
            time.sleep(delay_s)
        except Exception as e:
            logger.warning("Postgres indisponible (tentative %d/10) : %s", attempt, e)
        raise RuntimeError("Impossible de se connecter à Postgres")


def load_sportifs() -> dict[int, str]:
    """Retourne {id_salarie: sport} pour les salariés qui déclarent un sport."""
    df_sport = pd.read_excel(DATA_DIR / "donnees_sportive.xlsx").rename(
        columns=SPORT_COLUMNS
    )
    sportifs = df_sport.dropna(subset=["sport"])
    logger.info("%d salariés sur %d déclarent un sport", len(sportifs), len(df_sport))
    return dict(zip(sportifs["id_salarie"].astype(int), sportifs["sport"]))


# --- Génération --------------------------------------------------------------
def generate_effort(sport: str) -> tuple[int | None, timedelta]:
    """Distance (mètres, None si non pertinente) et durée d'une activité."""
    if sport in DISTANCE_SPORTS:
        dist_min, dist_max, speed_min, speed_max = DISTANCE_SPORTS[sport]
        distance_km = random.uniform(dist_min, dist_max)
        duration_h = distance_km / random.uniform(speed_min, speed_max)
        return round(distance_km * 1000), timedelta(hours=duration_h)

    duration_min, duration_max = DURATION_SPORTS.get(sport, DEFAULT_DURATION)
    return None, timedelta(minutes=random.randint(duration_min, duration_max))


def apply_anomaly(distance_m, date_debut, date_fin):
    """Rend l'activité incohérente (distance absurde, fin avant début ou durée excessive)."""
    kind = random.choice(["distance", "dates", "duree"])
    if kind == "distance":
        distance_m = (distance_m or 1000) * 100
    elif kind == "dates":
        date_debut, date_fin = date_fin, date_debut
    else:
        date_fin = date_debut + timedelta(hours=random.randint(20, 48))
    logger.debug("Anomalie injectée : %s", kind)
    return distance_m, date_debut, date_fin


def build_activite(id_salarie: int, sport: str, date_debut: datetime) -> tuple:
    distance_m, duration = generate_effort(sport)
    date_fin = date_debut + duration
    if random.random() < ANOMALY_RATE:
        distance_m, date_debut, date_fin = apply_anomaly(
            distance_m, date_debut, date_fin
        )
    commentaire = random.choice(COMMENTAIRES) if random.random() < 0.6 else None
    return (id_salarie, date_debut, sport, distance_m, date_fin, commentaire)


def random_start(day: date) -> datetime:
    """Heure de début plausible (entre 6h et 21h, heure de Paris) pour un jour donné."""
    return datetime(
        day.year,
        day.month,
        day.day,
        random.randint(6, 20),
        random.randint(0, 59),
        tzinfo=TZ,
    )


INSERT_ACTIVITE = """
    INSERT INTO activites (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def backfill(conn: psycopg.Connection, sportifs: dict[int, str]) -> None:
    """Génère l'historique des 12 derniers mois si la table activites est vide."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM activites")
        if cur.fetchone()[0] > 0:
            logger.info("Historique déjà présent, pas de backfill")
            return

        today = datetime.now(TZ).date()
        rows = []
        for id_salarie, sport in sportifs.items():
            for _ in range(
                random.randint(MIN_ACTIVITIES_PER_YEAR, MAX_ACTIVITIES_PER_YEAR)
            ):
                day = today - timedelta(days=random.randint(1, HISTORY_DAYS - 1))
                rows.append(build_activite(id_salarie, sport, random_start(day)))

        rows.sort(key=lambda r: r[1])  # insertion chronologique
        cur.executemany(INSERT_ACTIVITE, rows)
    conn.commit()
    logger.info(
        "Backfill : %d activités générées pour %d sportifs", len(rows), len(sportifs)
    )


def insert_live_activite(conn: psycopg.Connection, sportifs: dict[int, str]) -> None:
    """Insère une activité qui vient de se terminer."""
    id_salarie, sport = random.choice(list(sportifs.items()))
    _, duration = generate_effort(sport)
    row = build_activite(id_salarie, sport, datetime.now(TZ) - duration)
    with conn.cursor() as cur:
        cur.execute(INSERT_ACTIVITE, row)
    conn.commit()
    logger.info("Nouvelle activité : salarié %s, %s", id_salarie, sport)


def purge_old_activites(conn: psycopg.Connection) -> None:
    """Supprime les activités de plus d'un an."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM activites WHERE date_debut < now() - make_interval(days => %s)",
            (HISTORY_DAYS,),
        )
        deleted = cur.rowcount
    conn.commit()
    logger.info(
        "Purge : %d activités de plus de %d jours supprimées", deleted, HISTORY_DAYS
    )


def main() -> None:
    sportifs = load_sportifs()
    if not sportifs:
        raise RuntimeError("Aucun salarié ne pratique de sport : rien à générer")

    with connect() as conn:
        backfill(conn, sportifs)

        last_purge = None
        while True:
            today = datetime.now(TZ).date()
            if last_purge != today:
                purge_old_activites(conn)
                last_purge = today
            time.sleep(GENERATION_INTERVAL_S)
            insert_live_activite(conn, sportifs)


if __name__ == "__main__":
    main()
