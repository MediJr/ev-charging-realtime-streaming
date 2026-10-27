"""Paramètres partagés par les producteurs et le traitement Spark. Les secrets viennent de l'environnement (.env)."""

import os

# --- Kafka (Redpanda) : un topic par type de message ---------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC_TELEMETRY = "charger.telemetry"     # mesures des bornes, clé = charger_id (l'ordre est garanti par borne)
TOPIC_CO2 = "grid.co2"                    # intensité carbone RTE, clé = créneau (topic compacté)
TOPIC_ALERTS = "charger.alerts"           # alertes produites par Spark, pour d'autres consommateurs
TOPIC_INJECTIONS = "sim.injections"       # vérité terrain du simulateur (instrumentation de la démonstration)

# --- Règles d'anomalie (cahier des charges, section 4) ----------------------------------------------------------
SILENCE_SECONDS = 60          # borne muette : aucun message depuis plus de 60 s
OVERPOWER_RATIO = 1.10        # surpuissance : plus de 110 % de la puissance nominale
STUCK_SECONDS = 120           # borne bloquée : session ouverte à 0 kW depuis plus de 2 min
WATERMARK = "2 minutes"       # une mesure en retard de moins de 2 min compte encore
TELEMETRY_PERIOD_S = 5        # une borne envoie une mesure toutes les 5 s (cadrage, question 3)

# --- PostgreSQL (catalogue Iceberg et base de diffusion) ---------------------------------------------------------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "lumio")
PG_USER = os.getenv("POSTGRES_USER", "lumio")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# --- Lakehouse S3 ------------------------------------------------------------------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://lake:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "lumio-lake")

# --- API RTE (même source que le projet 2) -----------------------------------------------------------------------
ODRE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records"
CO2_POLL_SECONDS = int(os.getenv("CO2_POLL_SECONDS", "300"))


def pg_dsn() -> str:
    return f"host={PG_HOST} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} connect_timeout=10"
