-- Base de diffusion lue par Grafana (schéma serving). Exécuté une fois, à la création du conteneur PostgreSQL.
-- Le catalogue Iceberg, lui, crée et gère ses propres tables (iceberg_tables, iceberg_namespace_properties).
-- Toutes les écritures de Spark sont des UPSERT sur une clé naturelle : rejouer un micro-lot ne crée aucun doublon.

CREATE SCHEMA IF NOT EXISTS serving;

-- État courant de chaque borne (une ligne par borne)
CREATE TABLE IF NOT EXISTS serving.charger_status (
    charger_id      text PRIMARY KEY,
    site_id         text NOT NULL,
    rated_kw        numeric(6, 1) NOT NULL,
    last_event_time timestamptz NOT NULL,
    power_kw        numeric(8, 3) NOT NULL,
    energy_kwh      numeric(10, 4),           -- énergie délivrée depuis le début de la session en cours
    status          text NOT NULL,
    session_id      text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Alertes : une ligne par épisode d'anomalie, ouverte puis résolue
CREATE TABLE IF NOT EXISTS serving.alerts (
    charger_id    text NOT NULL,
    alert_type    text NOT NULL CHECK (alert_type IN ('SILENT', 'OVERPOWER', 'STUCK')),
    anomaly_time  timestamptz NOT NULL,     -- heure où l'anomalie est avérée (temps de l'événement)
    site_id       text NOT NULL,
    severity      text NOT NULL,
    detail        text,
    detected_at   timestamptz NOT NULL,     -- heure où Spark l'a détectée (temps de traitement)
    resolved_at   timestamptz,
    PRIMARY KEY (charger_id, alert_type, anomaly_time)
);
CREATE INDEX IF NOT EXISTS alerts_open ON serving.alerts (charger_id, alert_type) WHERE resolved_at IS NULL;

-- Énergie et CO₂ par borne et par minute close
CREATE TABLE IF NOT EXISTS serving.charger_minute (
    charger_id      text NOT NULL,
    window_start    timestamptz NOT NULL,
    site_id         text NOT NULL,
    session_id      text,
    avg_power_kw    numeric(8, 3) NOT NULL,
    energy_kwh      numeric(10, 4) NOT NULL,
    samples         integer NOT NULL,
    co2_g_per_kwh   numeric(6, 1),
    co2_g           numeric(12, 2),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (charger_id, window_start)
);
CREATE INDEX IF NOT EXISTS charger_minute_window ON serving.charger_minute (window_start);

-- Intensité carbone RTE (un créneau de 15 min par ligne)
CREATE TABLE IF NOT EXISTS serving.grid_co2 (
    slot            timestamptz PRIMARY KEY,
    co2_g_per_kwh   numeric(6, 1) NOT NULL,
    consumption_mw  integer,
    received_at     timestamptz NOT NULL DEFAULT now()
);

-- Progression des requêtes Spark (un micro-lot par ligne) : débit, durée, lignes écartées par la watermark
CREATE TABLE IF NOT EXISTS serving.stream_progress (
    query_name              text NOT NULL,
    batch_id                bigint NOT NULL,
    batch_time              timestamptz NOT NULL,
    input_rows              bigint NOT NULL,
    input_rows_per_second   double precision,
    processed_rows_per_sec  double precision,
    batch_duration_ms       bigint,
    rows_dropped_by_watermark bigint,
    PRIMARY KEY (query_name, batch_id)
);

-- Vérité terrain du simulateur (instrumentation de la démonstration, pas une donnée métier)
CREATE TABLE IF NOT EXISTS serving.sim_injections (
    kind         text NOT NULL,
    charger_id   text NOT NULL,
    start_time   timestamptz NOT NULL,
    event_id     text NOT NULL DEFAULT '',
    duration_s   double precision,
    PRIMARY KEY (kind, charger_id, start_time, event_id)
);

-- Q1 · Délai de détection : de l'anomalie avérée à l'alerte
CREATE OR REPLACE VIEW serving.v_detection_latency AS
SELECT alert_type, charger_id, anomaly_time, detected_at,
       extract(epoch FROM detected_at - anomaly_time) AS latency_s
FROM serving.alerts;

-- Q3 · CO₂ de chaque recharge (somme des minutes de la session)
CREATE OR REPLACE VIEW serving.v_session_co2 AS
SELECT session_id, charger_id, site_id,
       min(window_start) AS started_at, max(window_start) + interval '1 minute' AS last_minute,
       sum(energy_kwh) AS energy_kwh, sum(co2_g) AS co2_g,
       sum(co2_g) / nullif(sum(energy_kwh), 0) AS co2_g_per_kwh
FROM serving.charger_minute
WHERE session_id IS NOT NULL
GROUP BY session_id, charger_id, site_id;

-- Recette C2 · Chaque anomalie injectée a-t-elle été détectée ?
CREATE OR REPLACE VIEW serving.v_injection_detection AS
SELECT i.kind, i.charger_id, i.start_time, i.duration_s,
       a.anomaly_time, a.detected_at,
       extract(epoch FROM a.detected_at - a.anomaly_time) AS latency_s
FROM serving.sim_injections i
LEFT JOIN LATERAL (
    SELECT * FROM serving.alerts a
    WHERE a.charger_id = i.charger_id AND a.alert_type = i.kind
      AND a.anomaly_time BETWEEN i.start_time - interval '10 seconds' AND i.start_time + interval '5 minutes'
    ORDER BY a.anomaly_time LIMIT 1
) a ON true
WHERE i.kind IN ('SILENT', 'OVERPOWER', 'STUCK');

-- Q3 en direct · CO₂ des recharges en cours (énergie de la session × intensité RTE la plus récente)
CREATE OR REPLACE VIEW serving.v_live_sessions AS
SELECT s.charger_id, s.site_id, s.session_id, s.power_kw, s.energy_kwh, c.co2_g_per_kwh,
       s.energy_kwh * c.co2_g_per_kwh AS co2_g, s.last_event_time
FROM serving.charger_status s
CROSS JOIN LATERAL (SELECT co2_g_per_kwh FROM serving.grid_co2 ORDER BY slot DESC LIMIT 1) c
WHERE s.session_id IS NOT NULL AND s.last_event_time > now() - interval '2 minutes';
