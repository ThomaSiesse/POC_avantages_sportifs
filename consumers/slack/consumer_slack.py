"""
Consumer Slack : lit le topic Debezium des activités et félicite les salariés sur Slack.

1. Charge les noms des salariés (donnees_rh.xlsx).
2. Lit les nouvelles activités du topic Kafka.
3. Ignore les activités anciennes ou incohérentes.
4. Envoie un message de félicitations via le webhook Slack.
"""

import json
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from confluent_kafka import Consumer

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
MAX_AGE_H = float(os.getenv("MAX_AGE_H", "24"))  # pas de message pour l'historique
MAX_DURATION_H = 12
MAX_SPEED_KMH = 50  # au-delà, la distance est incohérente quel que soit le sport

# Messages par sport ({nom}, {distance}, {duree}) ; DEFAULT pour les autres
MESSAGES = {
    "Runing": "Bravo {nom} ! Tu viens de courir {distance} km en {duree} ! Quelle énergie ! 🔥🏅",
    "Randonnée": "Magnifique {nom} ! Une randonnée de {distance} km terminée et un nouveau spot à découvrir ! 🌄",
    "Natation": "Splash ! {nom} vient de nager {distance} km en {duree} ! 🏊💦",
    "Escalade": "Chapeau {nom} ! {duree} de grimpe, ça c'est du sommet ! 🧗⛰️",
}
DEFAULT_DISTANCE = "Bravo {nom} ! {distance} km de {sport} en {duree} ! 💪🏅"
DEFAULT_DUREE = "Bravo {nom} ! {duree} de {sport}, quelle séance ! 💪🏅"


def load_salaries() -> dict[int, str]:
    """Retourne {id_salarie: "Prénom Nom"} à partir des données RH."""
    df_rh = pd.read_excel(DATA_DIR / "donnees_rh.xlsx")
    noms = df_rh["Prénom"] + " " + df_rh["Nom"]
    return dict(zip(df_rh["ID salarié"].astype(int), noms))


def is_valid(activite: dict, duree: timedelta) -> bool:
    """Nouvelle activité (op = "c"), récente, avec une durée et une vitesse réalistes."""
    if activite.get("__op") != "c":
        return False
    if not timedelta(0) < duree <= timedelta(hours=MAX_DURATION_H):
        return False
    date_fin = datetime.fromisoformat(activite["date_fin"])
    if date_fin < datetime.now(UTC) - timedelta(hours=MAX_AGE_H):
        return False
    distance_m = activite["distance_m"]
    if distance_m is not None:
        vitesse_kmh = (distance_m / 1000) / (duree.total_seconds() / 3600)
        return 0 < vitesse_kmh <= MAX_SPEED_KMH
    return True


def format_duree(duree: timedelta) -> str:
    """46 min ou 1 h 12."""
    hours, minutes = divmod(round(duree.total_seconds() / 60), 60)
    return f"{hours} h {minutes:02d}" if hours else f"{minutes} min"


def build_message(activite: dict, nom: str, duree: timedelta) -> str:
    sport = activite["type_sport"]
    distance_m = activite["distance_m"]
    template = MESSAGES.get(sport) or (
        DEFAULT_DISTANCE if distance_m else DEFAULT_DUREE
    )
    message = template.format(
        nom=nom,
        sport=sport.lower(),
        distance=f"{(distance_m or 0) / 1000:.1f}".replace(".", ",").removesuffix(",0"),
        duree=format_duree(duree),
    )
    if activite["commentaire"]:
        message += f' ("{activite["commentaire"]}")'
    return message


def send_to_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        logger.info("[DRY RUN] %s", message)
        return
    response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    if response.ok:
        logger.info("Message envoyé : %s", message)
    else:
        logger.error("Erreur Slack %d : %s", response.status_code, response.text)


def main() -> None:
    salaries = load_salaries()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Abonné au topic %s", KAFKA_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error() or msg.value() is None:
            continue

        activite = json.loads(msg.value())
        nom = salaries.get(activite["id_salarie"])
        duree = datetime.fromisoformat(activite["date_fin"]) - datetime.fromisoformat(
            activite["date_debut"]
        )
        if nom and is_valid(activite, duree):
            send_to_slack(build_message(activite, nom, duree))


if __name__ == "__main__":
    main()
