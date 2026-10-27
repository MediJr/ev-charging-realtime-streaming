"""Maintenance du lakehouse Iceberg, à planifier une fois par jour.

    docker compose exec spark /opt/spark/bin/spark-submit --master local[2] spark/maintenance.py

Le streaming écrit un petit fichier par micro-lot et par partition : des milliers de petits fichiers par jour, que
chaque requête doit ouvrir un par un (le « problème des petits fichiers »). La maintenance :
1. supprime les mesures brutes de plus de 90 jours (cadrage, question 7) ;
2. compacte les petits fichiers en fichiers d'environ 128 Mo ;
3. expire les anciens instantanés (snapshots), dont les fichiers ne servent plus qu'au voyage dans le temps.

Iceberg gère la concurrence de façon optimiste : la maintenance peut tourner pendant que le streaming écrit.
"""

from __future__ import annotations

import sys

from streaming_job import CATALOG, build_spark

RETENTION_DAYS = 90
TABLES = ["bronze.telemetry_raw", "silver.telemetry", "silver.grid_co2", "gold.charger_minute", "gold.alerts"]


def files(spark, table: str) -> tuple[int, float]:
    row = spark.sql(f"SELECT count(*) AS n, coalesce(avg(file_size_in_bytes), 0) / 1024 AS kb FROM {CATALOG}.{table}.files").first()
    return row.n, row.kb


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"DELETE FROM {CATALOG}.bronze.telemetry_raw WHERE kafka_time < current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS")
    for table in TABLES:
        before = files(spark, table)
        spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', options => map('min-input-files', '5'))")
        spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', older_than => current_timestamp() - INTERVAL 1 DAYS, "
                  f"retain_last => 10)")
        after = files(spark, table)
        print(f"{table:<24} fichiers : {before[0]:>6} → {after[0]:>6}   taille moyenne : {before[1]:>8.1f} Ko → {after[1]:>8.1f} Ko")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
