"""Simulateur du parc de bornes de Lumio Recharge (décision D1 du cadrage).

    python -m lumio_stream.simulator

Chaque borne envoie une mesure toutes les SIM_INTERVAL_SECONDS : puissance, énergie de la session, état.
Pour éprouver le pipeline, le simulateur injecte VOLONTAIREMENT :
- trois anomalies métier : borne muette, surpuissance, borne bloquée ;
- trois défauts réseau : messages en double, en retard (moins de 2 min) et très en retard (au-delà de la watermark).

Chaque injection est publiée dans le topic sim.injections : c'est la vérité terrain qui permet, en recette,
de vérifier que chaque anomalie a été détectée, et en combien de temps.

La graine (SIM_SEED) fixe le comportement du parc. Les identifiants, eux, sont des UUID v4 tirés du système :
avec la graine, un redémarrage du simulateur aurait regénéré les MÊMES event_id que le démarrage précédent.
"""

from __future__ import annotations

import heapq
import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from . import config

log = logging.getLogger("simulator")

# Probabilités par mesure (une borne envoie 720 mesures par heure à 5 s d'intervalle)
P_SESSION_START = 0.004       # une borne libre démarre une recharge
P_ANOMALY = 0.0004            # une borne déclenche une anomalie (environ 1 fois toutes les 3,5 h par borne)
P_DUPLICATE = 0.01            # le boîtier renvoie le même message
P_LATE = 0.01                 # le réseau 4G retarde le message de 5 à 40 s (dans la watermark)
P_TOO_LATE = 0.002            # le réseau retarde le message de 3 à 5 min (au-delà de la watermark)


@dataclass
class Charger:
    charger_id: str
    site_id: str
    rated_kw: float
    session_id: str | None = None
    energy_kwh: float = 0.0
    target_kwh: float = 0.0
    anomaly: str | None = None
    anomaly_until: datetime | None = None

    def tick(self, now: datetime, rng: random.Random, interval: float) -> dict | None:
        """Fait avancer la borne d'un pas de temps. Renvoie sa mesure, ou None si elle est muette."""
        if self.anomaly and now >= self.anomaly_until:
            self.anomaly = None
        if self.anomaly == "SILENT":
            return None

        if self.session_id is None and rng.random() < P_SESSION_START:
            self.session_id = f"S-{uuid.uuid4().hex[:12]}"
            self.energy_kwh, self.target_kwh = 0.0, rng.uniform(8, 60)

        if self.session_id is None:
            power, status = 0.0, "available"
        elif self.anomaly == "STUCK":
            power, status = 0.0, "charging"
        elif self.anomaly == "OVERPOWER":
            power, status = self.rated_kw * rng.uniform(1.15, 1.40), "charging"
        else:
            power, status = self.rated_kw * rng.uniform(0.60, 0.95), "charging"

        self.energy_kwh += power * interval / 3600
        event = {
            "event_id": str(uuid.uuid4()),
            "charger_id": self.charger_id,
            "site_id": self.site_id,
            "rated_kw": self.rated_kw,
            "event_time": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "power_kw": round(power, 3),
            "energy_kwh": round(self.energy_kwh, 4),
            "status": status,
            "session_id": self.session_id,
        }
        if self.session_id and self.energy_kwh >= self.target_kwh and self.anomaly is None:
            self.session_id = None                         # recharge terminée
        return event

    def maybe_start_anomaly(self, now: datetime, rng: random.Random) -> dict | None:
        """Déclenche parfois une anomalie et renvoie sa description (vérité terrain)."""
        if self.anomaly or rng.random() >= P_ANOMALY:
            return None
        kind = rng.choice(["SILENT", "OVERPOWER", "STUCK"])
        if kind == "STUCK" and self.session_id is None:     # une borne ne peut être bloquée qu'en pleine recharge
            self.session_id = f"S-{uuid.uuid4().hex[:12]}"
            self.energy_kwh, self.target_kwh = 0.0, 30.0
        if kind == "OVERPOWER" and self.session_id is None:
            return None
        duration = {"SILENT": rng.uniform(120, 240), "OVERPOWER": rng.uniform(30, 60), "STUCK": rng.uniform(180, 300)}[kind]
        self.anomaly, self.anomaly_until = kind, now + timedelta(seconds=duration)
        return {"kind": kind, "charger_id": self.charger_id, "site_id": self.site_id,
                "start_time": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "duration_s": round(duration, 1)}


def build_fleet(n: int, rng: random.Random, first_id: int = 1) -> list[Charger]:
    """n bornes sur 12 sites : deux tiers en 22 kW (courant alternatif), un tiers en 50 kW (courant continu).

    first_id permet de lancer un second simulateur (test de charge) sans réutiliser les identifiants du premier.
    """
    return [Charger(f"CH-{i:04d}", f"SITE-{i % 12 + 1:02d}", 50.0 if rng.random() < 1 / 3 else 22.0)
            for i in range(first_id, first_id + n)]


def network(event: dict, rng: random.Random, now: float) -> list[tuple[float, str]]:
    """Ce que le réseau fait du message : [(heure d'envoi, défaut injecté), …]."""
    r = rng.random()
    if r < P_TOO_LATE:
        return [(now + rng.uniform(180, 300), "too_late")]
    if r < P_TOO_LATE + P_LATE:
        return [(now + rng.uniform(5, 40), "late")]
    if r < P_TOO_LATE + P_LATE + P_DUPLICATE:
        return [(now, ""), (now + rng.uniform(0.5, 3), "duplicate")]
    return [(now, "")]


def run() -> None:
    from confluent_kafka import Producer

    n = int(os.getenv("SIM_CHARGERS", "200"))
    interval = float(os.getenv("SIM_INTERVAL_SECONDS", "5"))
    rng = random.Random(int(os.getenv("SIM_SEED", "42")))
    fleet = build_fleet(n, rng, int(os.getenv("SIM_FIRST_ID", "1")))
    # linger.ms : regrouper les messages par petits lots ; acks=all et idempotence : aucun message perdu ni dupliqué par Kafka
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50,
                         "enable.idempotence": True, "acks": "all"})
    pending: list[tuple[float, int, str, bytes]] = []    # messages retenus par le « réseau », triés par heure d'envoi
    seq, sent = 0, 0
    log.info("Simulation de %d bornes, une mesure toutes les %.0f s", n, interval)

    def publish_injection(payload: dict) -> None:
        producer.produce(config.TOPIC_INJECTIONS, key=payload["charger_id"], value=json.dumps(payload))

    while True:
        tick_start = time.time()
        for i, charger in enumerate(fleet):
            # Les bornes sont réparties sur l'intervalle : pas de rafale toutes les 5 s.
            due = tick_start + interval * i / n
            # Une seule lecture de l'horloge par tour : l'échéance peut passer entre deux lectures (attente négative = plantage).
            while (wait := due - time.time()) > 0:
                time.sleep(min(0.01, wait))
            now = datetime.now(timezone.utc)
            injection = charger.maybe_start_anomaly(now, rng)
            if injection:
                publish_injection(injection)
            event = charger.tick(now, rng, interval)
            if event is None:
                continue
            payload = json.dumps(event).encode()
            for send_at, fault in network(event, rng, time.time()):
                if fault:
                    publish_injection({"kind": fault, "charger_id": event["charger_id"], "event_id": event["event_id"],
                                       "start_time": event["event_time"]})
                seq += 1
                heapq.heappush(pending, (send_at, seq, event["charger_id"], payload))
            while pending and pending[0][0] <= time.time():
                _, _, key, value = heapq.heappop(pending)
                producer.produce(config.TOPIC_TELEMETRY, key=key, value=value)
                sent += 1
            producer.poll(0)
        if sent and int(tick_start) % 60 < interval:
            log.info("%d messages envoyés", sent)
        producer.flush(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run()
