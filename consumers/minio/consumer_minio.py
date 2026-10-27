"""
Consumer MinIO : lit le topic Debezium des activités et les stocke brutes dans MinIO.

1. Lit les événements du topic Kafka (toutes les opérations, sans filtrage).
2. Les regroupe par lots (BATCH_SIZE messages ou BATCH_TIMEOUT_S secondes).
3. Ajoute chaque lot à la table Delta Lake s3://raw/activites.

Le nettoyage est fait ensuite par dbt (bronze → silver → gold).
"""

import json
import logging
import os
import time
from datetime import UTC, datetime

import pyarrow as pa
from confluent_kafka import Consumer
from deltalake import write_deltalake

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("consumer_minio")

# --- Configuration ---------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sport.public.activites")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-minio")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
BATCH_TIMEOUT_S = int(os.getenv("BATCH_TIMEOUT_S", "30"))

TABLE_URI = f"s3://{os.getenv('MINIO_BUCKET_RAW', 'raw')}/activites"
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minio123"),
    "AWS_REGION": "eu-west-3",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

SCHEMA = pa.schema(
    [
        ("id_activite", pa.int64()),
        ("id_salarie", pa.int64()),
        ("date_debut", pa.string()),
        ("type_sport", pa.string()),
        ("distance_m", pa.int64()),
        ("date_fin", pa.string()),
        ("commentaire", pa.string()),
        ("op", pa.string()),
        ("source_ts_ms", pa.int64()),  # date du changement dans Postgres
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def to_row(activite: dict) -> dict:
    """Message Debezium -> ligne de la table raw."""
    return {
        **{name: activite.get(name) for name in SCHEMA.names[:7]},
        "op": activite.get("__op"),
        "source_ts_ms": activite.get("__source_ts_ms"),
        "ingested_at": datetime.now(UTC),
    }


def write_batch(rows: list[dict]) -> None:
    write_deltalake(
        TABLE_URI,
        pa.Table.from_pylist(rows, schema=SCHEMA),
        mode="append",
        storage_options=STORAGE_OPTIONS,
    )
    logger.info("%d activités ajoutées à %s", len(rows), TABLE_URI)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Abonné au topic %s", KAFKA_TOPIC)

    rows = []
    last_flush = time.monotonic()
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error() and msg.value() is not None:
            rows.append(to_row(json.loads(msg.value())))

        lot_plein = len(rows) >= BATCH_SIZE
        delai_depasse = time.monotonic() - last_flush >= BATCH_TIMEOUT_S
        if rows and (lot_plein or delai_depasse):
            write_batch(rows)
            consumer.commit(asynchronous=False)
            rows = []
        if lot_plein or delai_depasse:
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
