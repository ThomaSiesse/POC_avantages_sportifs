"""
Consumer Slack : lit le topic Debezium des activités et félicite les salariés sur Slack.

1. Charge les noms des salariés (donnees_rh.xlsx).
2. Lit les événements du topic Kafka (JSON à plat grâce au SMT ExtractNewRecordState).
3. Ne garde que les nouvelles activités (op = "c"), récentes et cohérentes
   (dates dans l'ordre, durée et vitesse réalistes pour le sport).
4. Construit un message personnalisé et l'envoie via le webhook Slack.

Le nettoyage complet des données reste le rôle de dbt : ici les règles sont
volontairement simples pour notifier en temps réel.
"""

import json
import logging
import os
import random
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("consumer_slack")

# --- Configuration ---------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sport.public.activites")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-slack")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
DATA_DIR = Path(os.getenv("DATA_DIR", "../../data"))
# Les activités plus anciennes (backfill, snapshot Debezium) ne sont pas notifiées
MAX_AGE_H = float(os.getenv("MAX_AGE_H", "24"))
MAX_DURATION_H = float(os.getenv("MAX_DURATION_H", "12"))
SLACK_RETRIES = int(os.getenv("SLACK_RETRIES", "3"))

RH_COLUMNS = {"ID salarié": "id_salarie", "Prénom": "prenom", "Nom": "nom"}

# Vitesse moyenne maximale réaliste par sport (km/h), pour les sports avec distance
MAX_SPEED_KMH = {
    "Runing": 25,
    "Randonnée": 8,
    "Triathlon": 50,
    "Natation": 7,
    "Voile": 45,
    "Équitation": 20,
    "Tennis": 8,
    "Badminton": 8,
    "Tennis de table": 8,
    "Football": 12,
    "Basketball": 12,
    "Rugby": 12,
}

# Messages par sport : {nom}, {distance}, {duree} sont remplacés à l'envoi
MESSAGES = {
    "Runing": [
        "Bravo {nom} ! Tu viens de courir {distance} km en {duree} ! Quelle énergie ! 🔥🏅",
        "Belle foulée {nom} ! {distance} km avalés en {duree} ! 🏃‍♀️💨",
    ],
    "Randonnée": [
        "Magnifique {nom} ! Une randonnée de {distance} km terminée et un nouveau spot à découvrir ! 🌄",
        "Bravo {nom} ! {distance} km de randonnée en {duree}, quel grand bol d'air ! 🥾🌲",
    ],
    "Natation": [
        "Splash ! {nom} vient de nager {distance} km en {duree} ! 🏊‍♂️💦",
    ],
    "Triathlon": [
        "Incroyable {nom} ! Un triathlon de {distance} km bouclé en {duree} ! 🏊🚴🏃🏆",
    ],
    "Voile": [
        "Cap sur le large ! {nom} a navigué {distance} km en {duree} ! ⛵🌊",
    ],
    "Équitation": [
        "Belle balade {nom} ! {distance} km à cheval en {duree} ! 🐎",
    ],
    "Escalade": [
        "Chapeau {nom} ! {duree} de grimpe, ça c'est du sommet ! 🧗‍♀️⛰️",
    ],
    "Judo": [
        "Ippon ! {nom} vient d'enchaîner {duree} de judo ! 🥋💪",
    ],
    "Boxe": [
        "Uppercut ! {nom} sort de {duree} de boxe ! 🥊🔥",
    ],
}
DEFAULT_MESSAGES = {
    "distance": ["Bravo {nom} ! {distance} km de {sport} en {duree} ! 💪🏅"],
    "duree": ["Bravo {nom} ! {duree} de {sport}, quelle séance ! 💪🏅"],
}


# --- Référentiel salariés ---------------------------------------------------
def load_salaries() -> dict[int, str]:
    """Retourne {id_salarie: "Prénom Nom"} à partir des données RH."""
    df_rh = pd.read_excel(DATA_DIR / "donnees_rh.xlsx").rename(columns=RH_COLUMNS)
    noms = df_rh["prenom"].str.strip() + " " + df_rh["nom"].str.strip()
    logger.info("%d salariés chargés depuis les données RH", len(df_rh))
    return dict(zip(df_rh["id_salarie"].astype(int), noms))


# --- Filtrage ----------------------------------------------------------------
def parse_ts(value) -> datetime:
    """Debezium publie les TIMESTAMPTZ en ISO 8601 ("2026-10-26T09:12:00Z")."""
    return datetime.fromisoformat(value)


def check_activite(activite: dict, salaries: dict[int, str]) -> str | None:
    """Retourne la raison du rejet, ou None si l'activité peut être notifiée."""
    if activite.get("__op") != "c":
        return f"opération {activite.get('__op')} ignorée (seules les créations sont notifiées)"
    if activite.get("id_salarie") not in salaries:
        return f"salarié {activite.get('id_salarie')} inconnu des données RH"
    if not activite.get("type_sport"):
        return "type de sport manquant"

    try:
        date_debut = parse_ts(activite["date_debut"])
        date_fin = parse_ts(activite["date_fin"])
    except (KeyError, TypeError, ValueError):
        return "dates manquantes ou illisibles"

    duration = date_fin - date_debut
    if duration <= timedelta(0):
        return "date de fin antérieure à la date de début"
    if duration > timedelta(hours=MAX_DURATION_H):
        return f"durée supérieure à {MAX_DURATION_H:g} h"
    if date_fin < datetime.now(UTC) - timedelta(hours=MAX_AGE_H):
        return f"activité terminée depuis plus de {MAX_AGE_H:g} h"

    distance_m = activite.get("distance_m")
    if distance_m is not None:
        if distance_m <= 0:
            return "distance négative ou nulle"
        speed_kmh = (distance_m / 1000) / (duration.total_seconds() / 3600)
        max_speed = MAX_SPEED_KMH.get(activite["type_sport"])
        if max_speed is not None and speed_kmh > max_speed:
            return f"vitesse irréaliste ({speed_kmh:.1f} km/h > {max_speed} km/h)"
    return None


# --- Message Slack -----------------------------------------------------------
def format_distance(distance_m: int) -> str:
    """10800 -> "10,8" (format français, décimale supprimée si nulle)."""
    return f"{distance_m / 1000:.1f}".replace(".", ",").removesuffix(",0")


def format_duree(duration: timedelta) -> str:
    """46 min, 1 h ou 1 h 12."""
    minutes = round(duration.total_seconds() / 60)
    hours, minutes = divmod(minutes, 60)
    if hours == 0:
        return f"{minutes} min"
    return f"{hours} h {minutes:02d}" if minutes else f"{hours} h"


def build_message(activite: dict, salaries: dict[int, str]) -> str:
    sport = activite["type_sport"]
    distance_m = activite.get("distance_m")
    templates = MESSAGES.get(sport) or DEFAULT_MESSAGES[
        "distance" if distance_m else "duree"
    ]
    # Un template qui parle de distance n'est utilisable que si la distance est connue
    if distance_m is None:
        templates = [t for t in templates if "{distance}" not in t] or DEFAULT_MESSAGES["duree"]

    message = random.choice(templates).format(
        nom=salaries[activite["id_salarie"]],
        sport=sport.lower(),
        distance=format_distance(distance_m) if distance_m else "",
        duree=format_duree(parse_ts(activite["date_fin"]) - parse_ts(activite["date_debut"])),
    )
    if activite.get("commentaire"):
        message += f' ("{activite["commentaire"]}")'
    return message


def send_to_slack(message: str) -> bool:
    """Poste le message sur le webhook ; sans webhook configuré, se contente de le logger."""
    if not SLACK_WEBHOOK_URL.startswith("https://"):
        logger.info("[DRY RUN] %s", message)
        return True

    for attempt in range(1, SLACK_RETRIES + 1):
        try:
            response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
        except requests.RequestException as e:
            logger.warning("Slack injoignable (tentative %d/%d) : %s", attempt, SLACK_RETRIES, e)
            time.sleep(2**attempt)
            continue

        if response.status_code == 200:
            logger.info("Message envoyé : %s", message)
            return True
        if response.status_code == 429:  # rate limit Slack (~1 message/s)
            time.sleep(int(response.headers.get("Retry-After", 1)))
            continue
        logger.error("Slack a refusé le message (%d) : %s", response.status_code, response.text)
        return False

    logger.error("Échec d'envoi après %d tentatives : %s", SLACK_RETRIES, message)
    return False


# --- Boucle principale -------------------------------------------------------
def handle_message(raw_value: bytes | None, salaries: dict[int, str]) -> None:
    if raw_value is None:  # tombstone
        return
    try:
        activite = json.loads(raw_value)
    except json.JSONDecodeError:
        logger.warning("Message non JSON ignoré : %r", raw_value[:200])
        return

    reason = check_activite(activite, salaries)
    if reason:
        logger.debug("Activité %s rejetée : %s", activite.get("id_activite"), reason)
        return
    send_to_slack(build_message(activite, salaries))


def main() -> None:
    salaries = load_salaries()
    if not SLACK_WEBHOOK_URL.startswith("https://"):
        logger.warning("SLACK_WEBHOOK_URL non configuré : mode DRY RUN (messages loggés)")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Abonné au topic %s (groupe %s)", KAFKA_TOPIC, KAFKA_GROUP_ID)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Le topic n'existe pas tant que Debezium n'a rien publié
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.error("Erreur Kafka : %s", msg.error())
                continue
            handle_message(msg.value(), salaries)
            # Commit après traitement : un redémarrage ne fait pas perdre de message
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
