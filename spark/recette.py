"""Contrôles de recette sur le lakehouse et la base de diffusion (critères C2 à C6).

    docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g spark/recette.py

Rapprochement des volumes : chaque message reçu de Kafka doit être expliqué.
    messages bronze = mesures silver + doublons écartés + retardataires écartés (au-delà de la watermark)

Bronze et silver sont écrites par deux requêtes distinctes : à un instant donné, l'une peut avoir un micro-lot d'avance.
Le rapprochement porte donc sur un ensemble FIXE : les messages arrivés avant une heure de coupure, prise 3 minutes
avant le dernier micro-lot commun aux deux tables.
"""

from __future__ import annotations

import psycopg

from lumio_stream import config
from streaming_job import CATALOG, build_spark


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    q = lambda sql: spark.sql(sql).first()  # noqa: E731

    cutoff = q(f"""SELECT least((SELECT max(kafka_time) FROM {CATALOG}.bronze.telemetry_raw),
                                (SELECT max(kafka_time) FROM {CATALOG}.silver.telemetry)) - INTERVAL 3 MINUTES AS t""").t
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW b AS
                  SELECT get_json_object(payload, '$.event_id') AS event_id, kafka_partition, kafka_offset
                  FROM {CATALOG}.bronze.telemetry_raw WHERE kafka_time <= '{cutoff}'""")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW s AS
                  SELECT * FROM {CATALOG}.silver.telemetry WHERE event_id IN (SELECT event_id FROM b)""")
    bronze = q("SELECT count(*) AS n, count(DISTINCT event_id) AS ids, count(DISTINCT kafka_partition, kafka_offset) AS offsets FROM b")
    silver = q("SELECT count(*) AS n, count(DISTINCT event_id) AS ids FROM s")
    missing = [r.event_id for r in spark.sql("SELECT DISTINCT event_id FROM b LEFT ANTI JOIN s USING (event_id)").collect()]

    print(f"=== C4 / C5 · Rapprochement des volumes (messages arrivés avant {cutoff})")
    print(f"messages bronze                : {bronze.n:>9,}   (positions Kafka distinctes : {bronze.offsets:,})")
    print(f"identifiants distincts         : {bronze.ids:>9,}   → doublons écartés : {bronze.n - bronze.ids:,}")
    print(f"mesures silver                 : {silver.n:>9,}   (identifiants distincts : {silver.ids:,})")
    print(f"identifiants absents de silver : {len(missing):>9,}")
    ok = bronze.n == silver.n + (bronze.n - bronze.ids) + len(missing)
    print(f"équation : {bronze.n:,} = {silver.n:,} + {bronze.n - bronze.ids:,} + {len(missing):,}  → {'OK' if ok else 'ÉCART'}")
    print(f"doublons dans silver           : {silver.n - silver.ids}")
    print(f"message lu deux fois par Spark : {bronze.n - bronze.offsets}   (même partition et même position Kafka)")

    with psycopg.connect(config.pg_dsn()) as conn:
        injected = dict(conn.execute(
            """SELECT kind, count(*) FROM serving.sim_injections
               WHERE kind IN ('duplicate', 'late', 'too_late') AND start_time <= %s GROUP BY kind""", (cutoff,)).fetchall())
        too_late = {r[0] for r in conn.execute("SELECT event_id FROM serving.sim_injections WHERE kind = 'too_late'").fetchall()}
        print(f"injections avant la coupure    : {injected}")
        explained = sum(1 for e in missing if e in too_late)
        print(f"absents de silver expliqués par un retard injecté de 3 à 5 min : {explained:,} / {len(missing):,}")

        print("\n=== C2 / C3 · Détection des anomalies injectées (hors 3 dernières minutes, encore en cours)")
        rows = conn.execute("""
            SELECT kind, count(*) AS injected, count(detected_at) AS detected,
                   round(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_s)::numeric, 1) AS p50,
                   round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_s)::numeric, 1) AS p95,
                   round(max(latency_s)::numeric, 1) AS max
            FROM serving.v_injection_detection
            WHERE start_time < now() - interval '3 minutes'
            GROUP BY kind ORDER BY kind""").fetchall()
        print(f"{'type':<10}{'injectées':>10}{'détectées':>10}{'p50 (s)':>9}{'p95 (s)':>9}{'max (s)':>9}")
        for r in rows:
            print(f"{r[0]:<10}{r[1]:>10}{r[2]:>10}{r[3]!s:>9}{r[4]!s:>9}{r[5]!s:>9}")
        total = conn.execute("""SELECT round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_s)::numeric, 1)
                                FROM serving.v_injection_detection WHERE detected_at IS NOT NULL""").fetchone()[0]
        print(f"p95 toutes anomalies : {total} s (objectif < 30 s)")
        false_alerts = conn.execute("""
            SELECT count(*) FROM serving.alerts a
            WHERE NOT EXISTS (SELECT 1 FROM serving.sim_injections i
                              WHERE i.charger_id = a.charger_id AND i.kind = a.alert_type
                                AND a.anomaly_time BETWEEN i.start_time - interval '10 seconds'
                                                       AND i.start_time + interval '5 minutes')""").fetchone()[0]
        print(f"alertes sans anomalie injectée (fausses alertes) : {false_alerts}")

        print("\n=== C6 · CO₂ des recharges")
        print(conn.execute("""SELECT count(*), round(avg(co2_g_per_kwh), 1), round(sum(energy_kwh), 1), round(sum(co2_g) / 1000, 2)
                              FROM serving.v_session_co2""").fetchone(), "(sessions, g/kWh moyen, kWh, kg CO₂)")
    spark.stop()


if __name__ == "__main__":
    main()
