"""Traitement en continu : Kafka (Redpanda) → Spark Structured Streaming → Iceberg, PostgreSQL et Kafka.

    spark-submit spark/streaming_job.py          (lancé par le service « spark » de docker-compose.yml)

Six requêtes tournent en parallèle dans la même application :

| Requête         | Source            | Traitement                                              | Destination                              |
|-----------------|-------------------|---------------------------------------------------------|------------------------------------------|
| bronze          | charger.telemetry | aucun : le message tel que reçu                         | Iceberg bronze.telemetry_raw             |
| silver          | charger.telemetry | lecture du JSON, watermark 2 min, dédoublonnage         | Iceberg silver.telemetry                 |
| monitor         | charger.telemetry | état par borne, règles d'anomalie (lumio_stream.monitor) | PostgreSQL, Kafka charger.alerts, Iceberg |
| minutes         | charger.telemetry | fenêtres d'une minute, écrites une fois closes, + CO₂   | PostgreSQL, Iceberg gold.charger_minute  |
| grid_co2        | grid.co2          | intensité carbone RTE                                   | PostgreSQL, Iceberg silver.grid_co2      |
| injections      | sim.injections    | vérité terrain du simulateur (instrumentation)          | PostgreSQL                               |

Garanties : chaque requête a son point de reprise (checkpoint). Les écritures Iceberg du puits natif sont « exactly-once » ;
celles faites dans foreachBatch sont rendues idempotentes (UPSERT, ou ajout marqué par le numéro de micro-lot).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import datetime, timezone

import pandas as pd
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from lumio_stream import config, monitor

CHECKPOINTS = os.getenv("CHECKPOINT_DIR", "/checkpoints")
CATALOG = "lake"

TELEMETRY = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("charger_id", T.StringType()),
    T.StructField("site_id", T.StringType()),
    T.StructField("rated_kw", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("power_kw", T.DoubleType()),
    T.StructField("energy_kwh", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("session_id", T.StringType()),
])
CO2 = T.StructType([
    T.StructField("slot", T.StringType()),
    T.StructField("co2_g_per_kwh", T.DoubleType()),
    T.StructField("consumption_mw", T.IntegerType()),
])
INJECTION = T.StructType([
    T.StructField("kind", T.StringType()),
    T.StructField("charger_id", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("duration_s", T.DoubleType()),
])

# État mémorisé par borne, et lignes produites par le moniteur (alertes et état courant)
STATE = T.StructType([
    T.StructField("site_id", T.StringType()),
    T.StructField("last_event_time", T.TimestampType()),
    T.StructField("stuck_since", T.TimestampType()),
    T.StructField("open_alerts", T.StringType()),
])
MONITOR_OUT = T.StructType([
    T.StructField("record", T.StringType()),          # alert | status
    T.StructField("charger_id", T.StringType()),
    T.StructField("site_id", T.StringType()),
    T.StructField("alert_type", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("anomaly_time", T.TimestampType()),
    T.StructField("detail", T.StringType()),
    T.StructField("rated_kw", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("power_kw", T.DoubleType()),
    T.StructField("energy_kwh", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("session_id", T.StringType()),
])


# --------------------------------------------------------------------------------------------- session Spark
def build_spark() -> SparkSession:
    s3 = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName("lumio-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")                     # petit volume : 200 partitions par défaut seraient du gaspillage
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(s3, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{s3}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{s3}.uri", f"jdbc:postgresql://{config.PG_HOST}:5432/{config.PG_DB}")
        .config(f"{s3}.jdbc.user", config.PG_USER)
        .config(f"{s3}.jdbc.password", config.PG_PASSWORD)
        .config(f"{s3}.jdbc.schema-version", "V1")
        .config(f"{s3}.warehouse", f"s3://{config.LAKE_BUCKET}/warehouse")
        .config(f"{s3}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{s3}.s3.endpoint", config.S3_ENDPOINT)
        .config(f"{s3}.s3.path-style-access", "true")
        .config(f"{s3}.s3.access-key-id", config.S3_ACCESS_KEY)
        .config(f"{s3}.s3.secret-access-key", config.S3_SECRET_KEY)
        .config(f"{s3}.client.region", "us-east-1")
        .getOrCreate()
    )


def create_tables(spark: SparkSession) -> None:
    """Tables Iceberg du lakehouse. Partitionnées par jour : une requête sur une journée ne lit qu'une partition."""
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    props = "TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.telemetry_raw (
        kafka_key string, payload string, topic string, kafka_partition int, kafka_offset bigint,
        kafka_time timestamp, ingested_at timestamp) USING iceberg PARTITIONED BY (days(kafka_time)) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.silver.telemetry (
        event_id string, charger_id string, site_id string, rated_kw double, event_time timestamp,
        power_kw double, energy_kwh double, status string, session_id string, kafka_time timestamp)
        USING iceberg PARTITIONED BY (days(event_time)) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.silver.grid_co2 (
        slot timestamp, co2_g_per_kwh double, consumption_mw int, received_at timestamp) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.gold.charger_minute (
        charger_id string, site_id string, session_id string, window_start timestamp, window_end timestamp,
        avg_power_kw double, energy_kwh double, samples bigint, co2_g_per_kwh double, co2_g double)
        USING iceberg PARTITIONED BY (days(window_start)) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.gold.alerts (
        charger_id string, site_id string, alert_type string, state string, severity string,
        anomaly_time timestamp, detected_at timestamp, detail string)
        USING iceberg PARTITIONED BY (days(anomaly_time)) {props}""")


# --------------------------------------------------------------------------------------------- utilitaires
def pg() -> psycopg.Connection:
    return psycopg.connect(config.pg_dsn(), autocommit=False)


def utc(value: datetime | None) -> datetime | None:
    """Spark rend des horodatages sans fuseau, en UTC (session en UTC) : on le rend explicite pour PostgreSQL."""
    return value.replace(tzinfo=timezone.utc) if value is not None and value.tzinfo is None else value


def append_once(df: DataFrame, table: str, query: str, batch_id: int) -> None:
    """Ajout idempotent dans Iceberg depuis foreachBatch.

    foreachBatch garantit « au moins une fois » : après une panne, un micro-lot peut être rejoué. On marque chaque
    instantané (snapshot) Iceberg du numéro de micro-lot, et on n'écrit pas un lot déjà présent.
    """
    marker = f"{query}:{batch_id}"
    spark = df.sparkSession
    done = spark.sql(f"SELECT 1 FROM {table}.snapshots WHERE summary['lumio.batch'] = '{marker}' LIMIT 1").count()
    if not done:
        df.writeTo(table).option("snapshot-property.lumio.batch", marker).append()


def latest_co2() -> float | None:
    """Intensité carbone RTE la plus récente connue (décision 5 du cadrage)."""
    with pg() as conn:
        row = conn.execute("SELECT co2_g_per_kwh FROM serving.grid_co2 ORDER BY slot DESC LIMIT 1").fetchone()
    return float(row[0]) if row else None


def read_topic(spark: SparkSession, topic: str, max_offsets: int = 50_000) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")         # premier démarrage : tout l'historique du topic ; ensuite, le checkpoint
        .option("maxOffsetsPerTrigger", max_offsets)   # contre-pression : un rattrapage se fait par lots bornés
        .load()
    )


# --------------------------------------------------------------------------------------------- moniteur (état par borne)
def _state_in(state: GroupState) -> tuple[str | None, monitor.ChargerState]:
    if not state.exists:
        return None, monitor.ChargerState()
    site, last, stuck, open_alerts = state.get
    return site, monitor.ChargerState(
        last_event_time=pd.Timestamp(last).to_pydatetime() if last is not None else None,
        stuck_since=pd.Timestamp(stuck).to_pydatetime() if stuck is not None else None,
        open_alerts=[a for a in (open_alerts or "").split(",") if a],
    )


def _alert_rows(alerts: list[monitor.Alert]) -> list[dict]:
    return [{"record": "alert", **{k: v for k, v in a.as_row().items()}} for a in alerts]


def monitor_charger(key: tuple, batches: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    """Appelé par Spark pour chaque borne qui a reçu des mesures, ou dont le délai de silence a expiré."""
    charger_id = key[0]
    site, st = _state_in(state)
    rows: list[dict] = []
    if state.hasTimedOut:
        rows += _alert_rows(monitor.on_timeout(charger_id, site, st))
    else:
        events = pd.concat(list(batches)).sort_values("event_time")
        records = [
            {**r, "event_time": pd.Timestamp(r["event_time"]).to_pydatetime()} for r in events.to_dict("records")
        ]
        rows += _alert_rows(monitor.update(charger_id, records, st))
        last = events.iloc[-1]
        site = last["site_id"]
        rows.append({"record": "status", "charger_id": charger_id, "site_id": site, "rated_kw": last["rated_kw"],
                     "event_time": last["event_time"], "power_kw": last["power_kw"], "energy_kwh": last["energy_kwh"],
                     "status": last["status"],
                     "session_id": last["session_id"]})
        # Le compte à rebours du silence repart à chaque mesure reçue.
        state.setTimeoutDuration(config.SILENCE_SECONDS * 1000)
    state.update((site, st.last_event_time, st.stuck_since, ",".join(st.open_alerts)))
    if rows:
        out = pd.DataFrame(rows, columns=[f.name for f in MONITOR_OUT.fields])
        # Une colonne de dates entièrement vide (lot sans alerte) serait typée en nombre par pandas, et Arrow
        # refuserait de la convertir : on fixe le type explicitement.
        for col in ("anomaly_time", "event_time"):
            out[col] = pd.to_datetime(out[col])
        yield out


def write_monitor(batch: DataFrame, batch_id: int) -> None:
    rows = batch.collect()
    if not rows:
        return
    detected_at = datetime.now(timezone.utc)
    alerts = [r for r in rows if r.record == "alert"]
    statuses = {r.charger_id: r for r in rows if r.record == "status"}     # la plus récente par borne
    with pg() as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO serving.charger_status AS s
                   (charger_id, site_id, rated_kw, last_event_time, power_kw, energy_kwh, status, session_id, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
               ON CONFLICT (charger_id) DO UPDATE SET site_id = EXCLUDED.site_id, rated_kw = EXCLUDED.rated_kw,
                   last_event_time = EXCLUDED.last_event_time, power_kw = EXCLUDED.power_kw,
                   energy_kwh = EXCLUDED.energy_kwh, status = EXCLUDED.status,
                   session_id = EXCLUDED.session_id, updated_at = now()
               WHERE EXCLUDED.last_event_time >= s.last_event_time""",
            [(r.charger_id, r.site_id, r.rated_kw, utc(r.event_time), r.power_kw, r.energy_kwh, r.status, r.session_id)
             for r in statuses.values()],
        )
        for a in alerts:
            if a.state == "open":
                # ON CONFLICT DO NOTHING : un micro-lot rejoué ne modifie pas l'heure de détection d'origine.
                cur.execute(
                    """INSERT INTO serving.alerts (charger_id, alert_type, anomaly_time, site_id, severity, detail, detected_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (a.charger_id, a.alert_type, utc(a.anomaly_time), a.site_id, a.severity, a.detail, detected_at),
                )
            else:
                cur.execute(
                    """UPDATE serving.alerts SET resolved_at = %s
                       WHERE charger_id = %s AND alert_type = %s AND resolved_at IS NULL""",
                    (utc(a.anomaly_time), a.charger_id, a.alert_type),
                )
    alerts_df = (batch.where("record = 'alert'")
                 .select("charger_id", "site_id", "alert_type", "state", "severity", "anomaly_time", "detail")
                 .withColumn("detected_at", F.lit(detected_at)))
    if alerts:
        # Les alertes sont aussi publiées dans Kafka, pour d'autres consommateurs (SMS, ticketing…)
        (alerts_df.selectExpr("charger_id AS key", "to_json(struct(*)) AS value")
         .write.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
         .option("topic", config.TOPIC_ALERTS).save())
        append_once(alerts_df.select("charger_id", "site_id", "alert_type", "state", "severity", "anomaly_time",
                                     "detected_at", "detail"), f"{CATALOG}.gold.alerts", "monitor", batch_id)


# --------------------------------------------------------------------------------------------- minutes et CO₂
def minute_aggregates(telemetry: DataFrame) -> DataFrame:
    """Énergie par borne et par minute. Une mesure toutes les 5 s : énergie = somme des puissances × 5 s."""
    return (
        telemetry.groupBy(F.window("event_time", "1 minute"), "charger_id", "site_id")
        .agg(
            F.avg("power_kw").alias("avg_power_kw"),
            (F.sum("power_kw") * config.TELEMETRY_PERIOD_S / 3600).alias("energy_kwh"),
            F.count("*").alias("samples"),
            F.max_by("session_id", "event_time").alias("session_id"),
        )
        .select("charger_id", "site_id", "session_id", F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "avg_power_kw", "energy_kwh", "samples")
    )


def with_co2(batch: DataFrame) -> DataFrame:
    intensity = latest_co2()
    return (batch.withColumn("co2_g_per_kwh", F.lit(intensity).cast("double"))
            .withColumn("co2_g", F.col("energy_kwh") * F.col("co2_g_per_kwh")))


def write_minutes(batch: DataFrame, batch_id: int) -> None:
    """Une minute close : dans PostgreSQL (UPSERT) et dans Iceberg (ajout marqué par le numéro de micro-lot)."""
    enriched = with_co2(batch)
    rows = enriched.collect()
    if not rows:
        return
    with pg() as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO serving.charger_minute (charger_id, window_start, site_id, session_id, avg_power_kw,
                   energy_kwh, samples, co2_g_per_kwh, co2_g, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
               ON CONFLICT (charger_id, window_start) DO UPDATE SET session_id = EXCLUDED.session_id,
                   avg_power_kw = EXCLUDED.avg_power_kw, energy_kwh = EXCLUDED.energy_kwh, samples = EXCLUDED.samples,
                   co2_g_per_kwh = EXCLUDED.co2_g_per_kwh, co2_g = EXCLUDED.co2_g, updated_at = now()""",
            [(r.charger_id, utc(r.window_start), r.site_id, r.session_id, r.avg_power_kw, r.energy_kwh, r.samples,
              r.co2_g_per_kwh, r.co2_g) for r in rows],
        )
    append_once(enriched, f"{CATALOG}.gold.charger_minute", "minutes", batch_id)


# --------------------------------------------------------------------------------------------- CO₂ et vérité terrain
def write_co2(batch: DataFrame, batch_id: int) -> None:
    rows = batch.collect()
    if not rows:
        return
    with pg() as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO serving.grid_co2 (slot, co2_g_per_kwh, consumption_mw) VALUES (%s, %s, %s)
               ON CONFLICT (slot) DO UPDATE SET co2_g_per_kwh = EXCLUDED.co2_g_per_kwh,
                   consumption_mw = EXCLUDED.consumption_mw, received_at = now()""",
            [(utc(r.slot), r.co2_g_per_kwh, r.consumption_mw) for r in rows],
        )
    append_once(batch.withColumn("received_at", F.current_timestamp()), f"{CATALOG}.silver.grid_co2", "grid_co2", batch_id)


def write_injections(batch: DataFrame, batch_id: int) -> None:
    rows = batch.collect()
    if not rows:
        return
    with pg() as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO serving.sim_injections (kind, charger_id, start_time, event_id, duration_s)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            [(r.kind, r.charger_id, utc(r.start_time), r.event_id or "", r.duration_s) for r in rows],
        )


# --------------------------------------------------------------------------------------------- suivi des micro-lots
class ProgressToPostgres(StreamingQueryListener):
    """Enregistre chaque micro-lot (débit, durée, lignes écartées par la watermark) : la page « santé » de Grafana."""

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators) if p.stateOperators else 0
        try:
            with pg() as conn:
                conn.execute(
                    """INSERT INTO serving.stream_progress VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    (p.name, p.batchId, p.timestamp, p.numInputRows, p.inputRowsPerSecond, p.processedRowsPerSecond,
                     p.batchDuration, dropped),
                )
        except psycopg.Error:
            pass                         # le suivi ne doit jamais arrêter le traitement


# --------------------------------------------------------------------------------------------- transformations (testées)
def parse_telemetry(messages: DataFrame) -> DataFrame:
    """Messages (colonnes value, kafka_time) → mesures typées. Un message illisible est écarté."""
    return (
        messages.select(F.from_json("value", TELEMETRY).alias("e"), "kafka_time")
        .select("e.*", "kafka_time")
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .where("event_id IS NOT NULL AND event_time IS NOT NULL")
    )


def clean_telemetry(telemetry: DataFrame) -> DataFrame:
    """Watermark : Spark attend les retardataires 2 min ; au-delà, une mesure est écartée (et comptée).
    Dédoublonnage : un message renvoyé par le boîtier (même event_id) ne passe qu'une fois."""
    return telemetry.withWatermark("event_time", config.WATERMARK).dropDuplicatesWithinWatermark(["event_id"])


def monitor_stream(clean: DataFrame) -> DataFrame:
    """État par borne et règles d'anomalie : une alerte ou un état courant par ligne produite."""
    return clean.groupBy("charger_id").applyInPandasWithState(
        monitor_charger, MONITOR_OUT, STATE, "append", GroupStateTimeout.ProcessingTimeTimeout)


# --------------------------------------------------------------------------------------------- assemblage
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    spark.streams.addListener(ProgressToPostgres())

    raw = read_topic(spark, config.TOPIC_TELEMETRY)
    clean = clean_telemetry(parse_telemetry(raw.select(F.col("value").cast("string").alias("value"),
                                                       F.col("timestamp").alias("kafka_time"))))

    def start(df, name: str, trigger: str, **kwargs):
        writer = df.writeStream.queryName(name).trigger(processingTime=trigger) \
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
        for k, v in kwargs.items():
            writer = getattr(writer, k)(v)
        return writer

    start(raw.select(F.col("key").cast("string").alias("kafka_key"), F.col("value").cast("string").alias("payload"),
                     "topic", F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                     F.col("timestamp").alias("kafka_time"), F.current_timestamp().alias("ingested_at")),
          "bronze", "30 seconds", format="iceberg", outputMode="append").toTable(f"{CATALOG}.bronze.telemetry_raw")

    start(clean, "silver", "30 seconds", format="iceberg", outputMode="append").toTable(f"{CATALOG}.silver.telemetry")

    start(monitor_stream(clean), "monitor", "5 seconds", outputMode="append", foreachBatch=write_monitor).start()

    # Mode append : une minute n'est écrite qu'une fois close (fin de minute + watermark). Le « direct » passe par
    # serving.charger_status, mis à jour toutes les 5 s par le moniteur.
    start(minute_aggregates(clean), "minutes", "30 seconds", outputMode="append", foreachBatch=write_minutes).start()

    co2 = (read_topic(spark, config.TOPIC_CO2)
           .select(F.from_json(F.col("value").cast("string"), CO2).alias("c")).select("c.*")
           .withColumn("slot", F.col("slot").cast("timestamp")))
    start(co2, "grid_co2", "30 seconds", outputMode="append", foreachBatch=write_co2).start()

    injections = (read_topic(spark, config.TOPIC_INJECTIONS)
                  .select(F.from_json(F.col("value").cast("string"), INJECTION).alias("i")).select("i.*")
                  .withColumn("start_time", F.col("start_time").cast("timestamp")))
    start(injections, "injections", "30 seconds", outputMode="append", foreachBatch=write_injections).start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
