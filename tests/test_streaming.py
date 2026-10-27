"""Transformations Spark de bout en bout, sans Kafka : des fichiers de messages jouent le rôle du topic.

Exécuté dans l'image Spark (pyspark présent) ; ignoré ailleurs.
"""

import json
import time
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import streaming_job as job  # noqa: E402

T0 = datetime(2026, 10, 26, 10, 0, 0)


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def message(i: int, seconds: float, power: float, charger: str = "CH-1", event_id: str | None = None) -> str:
    return json.dumps({
        "event_id": event_id or f"e-{charger}-{i}", "charger_id": charger, "site_id": "SITE-01", "rated_kw": 22.0,
        "event_time": (T0 + timedelta(seconds=seconds)).isoformat() + "Z", "power_kw": power,
        "energy_kwh": i * 0.02, "status": "charging", "session_id": "S-1",
    })


def run_stream(spark, tmp_path, lines: list[str], transform, available_now: bool = True):
    """Écrit les messages dans un dossier, les lit comme un flux, applique la transformation, renvoie le résultat.

    available_now : traite tout puis s'arrête, y compris le micro-lot « sans données » qui clôt les fenêtres.
    Le moniteur a des délais de silence par borne : Spark enchaîne alors des micro-lots sans données pour les surveiller,
    et la requête ne s'arrêterait jamais. On attend dans ce cas le traitement des données présentes, puis on l'arrête.
    """
    source = tmp_path / "in"
    source.mkdir()
    (source / "part.json").write_text("\n".join(lines) + "\n", encoding="utf-8")
    stream = spark.readStream.text(str(source)).withColumn("kafka_time", F.current_timestamp())
    name = f"q_{tmp_path.name}".replace("-", "_")
    writer = (transform(job.clean_telemetry(job.parse_telemetry(stream))).writeStream.format("memory").queryName(name)
              .outputMode("append").option("checkpointLocation", str(tmp_path / "ckpt")))
    if available_now:
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    else:
        query = writer.start()
        deadline = time.time() + 120
        # Attendre le micro-lot qui a lu les données, plus un suivant (preuve qu'il est terminé)
        while time.time() < deadline:
            progress = query.recentProgress
            done = [i for i, p in enumerate(progress) if p["numInputRows"] > 0]
            if done and len(progress) > done[0] + 1:
                break
            time.sleep(1)
        query.stop()
    return spark.table(name).collect()


def test_duplicates_are_removed(spark, tmp_path):
    lines = [message(i, i * 5, 18) for i in range(10)] + [message(3, 15, 18), message(7, 35, 18)]     # 2 renvois
    rows = run_stream(spark, tmp_path, lines, lambda df: df)
    assert len(rows) == 10


def test_unreadable_messages_are_dropped(spark, tmp_path):
    rows = run_stream(spark, tmp_path, [message(1, 0, 18), "{pas du json", '{"charger_id": "CH-1"}'], lambda df: df)
    assert len(rows) == 1


def test_monitor_raises_an_overpower_alert_through_spark_state(spark, tmp_path):
    lines = [message(i, i * 5, 30 if 4 <= i <= 8 else 18) for i in range(12)]
    rows = run_stream(spark, tmp_path, lines, job.monitor_stream, available_now=False)
    alerts = [(r.alert_type, r.state, r.anomaly_time) for r in rows if r.record == "alert"]
    assert alerts == [("OVERPOWER", "open", T0 + timedelta(seconds=20)), ("OVERPOWER", "resolved", T0 + timedelta(seconds=45))]
    status = [r for r in rows if r.record == "status"]
    assert status and status[-1].charger_id == "CH-1"


def test_monitor_handles_batches_without_any_alert(spark, tmp_path):
    # Régression : un lot sans alerte donnait une colonne de dates vide, typée en nombre, refusée par Arrow.
    lines = [message(i, i * 5, 18, charger=f"CH-{i % 3}") for i in range(9)]
    rows = run_stream(spark, tmp_path, lines, job.monitor_stream, available_now=False)
    assert {r.record for r in rows} == {"status"} and len({r.charger_id for r in rows}) == 3


def test_minute_energy_from_five_second_samples(spark, tmp_path):
    # 12 mesures à 12 kW sur une minute : 12 × 12 kW × 5 s = 0,2 kWh. La minute n'est émise qu'une fois close :
    # une mesure 3 min plus tard fait avancer la watermark au-delà de sa fin.
    lines = [message(i, i * 5, 12) for i in range(12)] + [message(99, 200, 12)]
    rows = run_stream(spark, tmp_path, lines, job.minute_aggregates)
    first = [r for r in rows if r.window_start == T0]
    assert len(first) == 1 and first[0].samples == 12
    assert first[0].energy_kwh == pytest.approx(0.2)
