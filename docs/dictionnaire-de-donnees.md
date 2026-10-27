# Dictionnaire de données

> Version 1.0.0 · 2026-10-26. Horodatages en **UTC**. Puissances en kW, énergies en kWh, intensité carbone en g CO₂/kWh.

## Topics Kafka (Redpanda)

| Topic | Partitions | Clé | Rétention | Producteur → consommateur |
|---|---|---|---|---|
| `charger.telemetry` | 6 | `charger_id` | 7 jours | Simulateur → Spark |
| `grid.co2` | 1 | créneau | compacté | Collecteur RTE → Spark |
| `charger.alerts` | 3 | `charger_id` | défaut | Spark → futurs consommateurs |
| `sim.injections` | 1 | `charger_id` | défaut | Simulateur → Spark (instrumentation) |

### Message `charger.telemetry` (JSON)

| Champ | Type | Description |
|---|---|---|
| `event_id` | texte (UUID) | Identifiant unique de la mesure : clé de dédoublonnage |
| `charger_id`, `site_id` | texte | Borne (`CH-0001`…) et site (`SITE-01`…`SITE-12`) |
| `rated_kw` | nombre | Puissance nominale : 22 (courant alternatif) ou 50 (courant continu) |
| `event_time` | texte ISO 8601 (UTC, `Z`) | Heure de la mesure, **sur la borne** (temps d'événement) |
| `power_kw` | nombre | Puissance instantanée |
| `energy_kwh` | nombre | Énergie délivrée depuis le début de la session |
| `status` | texte | `available` ou `charging` |
| `session_id` | texte ou null | Session de recharge en cours |

## Lakehouse Iceberg (catalogue `lake`, bucket `lumio-lake`)

| Table | Grain | Partition | Contenu |
|---|---|---|---|
| `bronze.telemetry_raw` | un message Kafka | jour de `kafka_time` | Message brut (`payload`), clé, topic, `kafka_partition`, `kafka_offset`, `kafka_time`, `ingested_at` |
| `silver.telemetry` | une mesure (dédoublonnée) | jour de `event_time` | Champs du message typés, plus `kafka_time` |
| `silver.grid_co2` | un créneau RTE de 15 min | — | `slot`, `co2_g_per_kwh`, `consumption_mw`, `received_at` |
| `gold.charger_minute` | borne × minute close | jour de `window_start` | `avg_power_kw`, `energy_kwh` (somme des puissances × 5 s), `samples`, `session_id`, `co2_g_per_kwh`, `co2_g` |
| `gold.alerts` | un changement d'état d'alerte | jour de `anomaly_time` | `alert_type`, `state` (open/resolved), `severity`, `anomaly_time`, `detected_at`, `detail` |

## Base de diffusion PostgreSQL (schéma `serving`)

| Table ou vue | Clé | Contenu |
|---|---|---|
| `charger_status` | `charger_id` | Dernière mesure de chaque borne (puissance, énergie de la session, état), mise à jour toutes les 5 s |
| `alerts` | (`charger_id`, `alert_type`, `anomaly_time`) | Un épisode d'anomalie : `detected_at` (heure de détection), `resolved_at` |
| `charger_minute` | (`charger_id`, `window_start`) | Énergie et CO₂ par minute close |
| `grid_co2` | `slot` | Intensité carbone RTE |
| `stream_progress` | (`query_name`, `batch_id`) | Un micro-lot Spark : lignes lues, débit, durée, lignes écartées par la watermark |
| `sim_injections` | (`kind`, `charger_id`, `start_time`, `event_id`) | Vérité terrain du simulateur (instrumentation) |
| `v_detection_latency` | — | Q1 · délai `detected_at − anomaly_time` par alerte |
| `v_live_sessions` | — | Q3 en direct · énergie et CO₂ des recharges en cours |
| `v_session_co2` | — | Q3 · CO₂ de chaque recharge, à partir des minutes closes |
| `v_injection_detection` | — | Recette · chaque anomalie injectée, et l'alerte correspondante |

## Règles d'anomalie

| Type | Gravité | Règle | `anomaly_time` |
|---|---|---|---|
| `SILENT` | critical | Aucune mesure depuis 60 s (délai de traitement de l'état Spark) | dernière mesure + 60 s |
| `OVERPOWER` | high | `power_kw` > `rated_kw` × 1,10 | heure de la mesure qui dépasse |
| `STUCK` | medium | Session ouverte et `power_kw` = 0 depuis 120 s | début de la période à 0 kW + 120 s |
