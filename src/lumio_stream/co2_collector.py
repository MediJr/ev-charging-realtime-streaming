"""Collecteur de l'intensité carbone RTE (éCO2mix temps réel) vers le topic grid.co2.

    python -m lumio_stream.co2_collector

Toutes les CO2_POLL_SECONDS, il lit les derniers créneaux publiés par RTE et publie ceux qu'il n'a pas encore envoyés.
Le topic est compacté et la clé est le créneau : republier un créneau remplace l'ancienne valeur, sans doublon.
Si l'API ne répond pas, il réessaie au tour suivant : Spark continue avec la dernière intensité connue.
"""

from __future__ import annotations

import json
import logging
import time

import requests

from . import config

log = logging.getLogger("co2_collector")


def fetch_latest(session: requests.Session, limit: int = 8) -> list[dict]:
    """Les derniers créneaux mesurés (taux de CO₂ renseigné), du plus ancien au plus récent."""
    params = {
        "select": "date_heure,taux_co2,consommation",
        "where": "taux_co2 is not null",
        "order_by": "date_heure desc",
        "limit": limit,
    }
    response = session.get(config.ODRE_URL, params=params, timeout=30)
    response.raise_for_status()
    rows = response.json()["results"]
    return [
        {"slot": r["date_heure"], "co2_g_per_kwh": float(r["taux_co2"]), "consumption_mw": r.get("consommation")}
        for r in reversed(rows)
    ]


def run() -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    session = requests.Session()
    session.headers["User-Agent"] = "portfolio-streaming-energie/1.0 (projet pedagogique, usage non commercial)"
    published: set[str] = set()
    while True:
        try:
            for row in fetch_latest(session):
                if row["slot"] not in published:
                    producer.produce(config.TOPIC_CO2, key=row["slot"], value=json.dumps(row))
                    published.add(row["slot"])
                    log.info("créneau %s : %.0f g/kWh", row["slot"], row["co2_g_per_kwh"])
            producer.flush(10)
        except requests.RequestException as error:
            log.warning("API RTE indisponible (%s) : nouvel essai dans %d s", error, config.CO2_POLL_SECONDS)
        time.sleep(config.CO2_POLL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run()
