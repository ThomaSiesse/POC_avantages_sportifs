"""
Lit le topic Kafka et envoie les messages dans Slack.
Les messages sont filtrés pour ne garder que les activités cohérentes (distance et vitesse réalistes).
Les messages sont envoyés dans Slack via un webhook, et sont également stockés dans Postgres.
"""

import pandas as pd
import requests
from kafka import KafkaConsumer
import os
import logging

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("consumer_slack")

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "consumer_slack"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode("utf-8"),
)
