"""
Lit le topic Kafka et envoie les messages dans Slack.
Les messages sont filtrés pour ne garder que les activités cohérentes (distance et vitesse réalistes).
Les messages sont envoyés dans Slack via un webhook, et sont également stockés dans Postgres.
"""

import pandas as pd
import requests
from kafka import KafkaConsumer
