# Rapport d'endurance : 14 heures de fonctionnement après la recette

> 🎭 **Scénario fictif** : voir le [brief client](../mission/2026-10-26-01-brief-client.md). Mission LUM-2026-002, version 1.0.1.

| | |
|---|---|
| **Date** | 2026-10-28 |
| **Période observée** | 2026-10-27 19:12 → 2026-10-28 09:18 UTC (14 h 06 min) |
| **Environnement** | La plateforme de la [recette](2026-10-27-pv-recette.md), laissée en marche sur un ordinateur portable, puis arrêtée |
| **Contenu de la période** | Recette, arrêt brutal de Spark, test de charge, vérification du rattrapage, une nuit et une matinée de fonctionnement |

> 🇬🇧 **Summary.** After acceptance, the platform ran for 14 hours on a laptop, including a night of host sleep. Over **1.6 million messages**,
> reconciliation stayed **exact** (0 lost, 0 duplicated) through at least 4 host suspensions and 2 automatic Spark restarts. In steady state, **84/84**
> anomalies were detected (p95 12.5 s). Compaction cut silver from 584 files to 3. The run also exposed a real flaw: after each host
> wake-up, the event-time silence safety net flagged **every charger** as silent at once (**1,458 false alerts** in 9 bursts). The fix,
> fleet-wide incident detection, is specified below.

---

## 1. Ce que la machine a vécu

La plateforme est restée allumée, mais l'ordinateur hôte s'est mis **en veille** plusieurs fois. Dans ce cas, la machine virtuelle de Docker
est figée : **tous** les conteneurs s'arrêtent, simulateur compris.

| Interruption (UTC) | Durée | Cause |
|---|---|---|
| 21:01 → 21:05 | 4 min | Arrêt volontaire de Spark (vérification du rattrapage) |
| 21:52 → 06:44 | **8 h 52** | Veille de l'ordinateur (nuit) |
| 08:12 → 08:21, 08:23 → 08:33, 08:46 → 09:00 | 9, 10, 14 min | Veilles de l'ordinateur |

**Preuve que ce sont des veilles de l'hôte, et pas des pannes de Spark** : les trous dans le journal du simulateur, qui ne dépend de rien,
coïncident exactement avec ceux de Spark. Temps de fonctionnement effectif : environ **4 h 40** sur 14 h 06.

**Redémarrages** : Spark s'est relancé **2 fois** (08:14 et 09:02), automatiquement, juste après un réveil. Cause : le canal de communication
entre Python et la JVM (`Py4J`) coupé par la veille, la même que lors des 31 h d'endurance du 26 octobre. Aucun autre conteneur n'a redémarré.

**Mémoire** à l'arrêt : Spark 2,1 Go sur 2,44 Go autorisés, Redpanda 728 Mo, PostgreSQL 70 Mo.

## 2. Intégrité des données : exacte sur 1,6 million de messages

Contrôle `spark/recette.py`, sur tous les messages arrivés avant 09:17:29 :

| Mesure | Valeur |
|---|---|
| Messages bronze | **1 606 357** (positions Kafka distinctes : 1 606 357) |
| Doublons écartés | 15 973 |
| Mesures silver | 1 588 693 (aucun doublon) |
| Retardataires écartés (au-delà de la watermark) | 1 691, dont **1 691 / 1 691** correspondent à un retard injecté de 3 à 5 min |
| Équation | **1 606 357 = 1 588 693 + 15 973 + 1 691 → OK** |
| Message Kafka lu deux fois | **0** |

Ces chiffres incluent le test de charge (environ 2 960 messages/s pendant 5 min). Malgré au moins 4 veilles de l'ordinateur, 1 arrêt brutal, 1 arrêt volontaire et
2 relances automatiques, **aucune mesure n'a été perdue ni comptée deux fois**.

## 3. Détection des anomalies

### En régime stable (06:46 → 08:08, aucune veille, aucune relance, aucune charge)

| Type | Injectées | Détectées | Médiane | 95e centile | Maximum |
|---|---|---|---|---|---|
| Surpuissance | 19 | 19 | 4,6 s | 6,1 s | 6,5 s |
| Borne bloquée | 27 | 27 | 8,0 s | 10,7 s | 10,8 s |
| Borne muette | 38 | 38 | 8,1 s | 24,8 s | 34,3 s |
| **Total** | **84** | **84** | **7,1 s** | **12,5 s** | 34,3 s |

La recette (27 anomalies, p95 de 11,6 s) est confirmée sur un échantillon trois fois plus grand. Une seule détection dépasse 30 s.

### Sur toute la période : 251 / 256

| Heure (UTC) | Anomalies | Plus de 30 s | Non détectées | Contexte |
|---|---|---|---|---|
| 19 h | 38 | 9 | 0 | Test de charge à partir de 19:53 |
| 20 h | 48 | 5 | 3 | Rattrapage après la charge (défaut corrigé à 21 h) |
| 21 h | 50 | 5 | 1 | Arrêt volontaire de Spark, puis veille |
| 06 h | 11 | 0 | 0 | Régime stable |
| 07 h | 62 | 1 | 0 | Régime stable |
| 08 h | 26 | 3 | 1 | Trois veilles, une relance |
| 09 h | 21 | 2 | 0 | Relance de 09:02 |

**Les 5 anomalies non détectées** :
- **3** pendant le rattrapage de 20 h : le défaut « borne muette manquée au rattrapage », corrigé puis vérifié ([journal](../journal/2026-10-26.md)) ;
- **2** bornes bloquées dont l'anomalie a commencé **moins de 2 minutes avant une veille**. Le simulateur était figé lui aussi ; à son réveil,
  l'anomalie avait expiré. Dans les données, ces bornes n'ont **jamais** été à 0 kW pendant 2 minutes : il n'y avait rien à détecter.
  Le pipeline a, à juste titre, signalé le trou de la veille comme un silence.

## 4. ⚠️ Défaut découvert : une rafale d'alertes après une coupure de la plateforme

| Alertes « borne muette » | Nombre |
|---|---|
| Isolées (une borne à la fois) | **106**, pour 103 silences injectés |
| En rafale (20 bornes ou plus dans la même minute, 9 rafales) | **1 458** |

**Cause.** Le filet de sécurité ajouté le 27 octobre signale un silence quand une borne reprend après un trou de plus de 60 s **dans le temps
des événements**. Après chaque veille, **les 200 bornes** reprennent en même temps après un trou de plusieurs minutes : chacune est déclarée
muette. Le pipeline confond une **coupure de la plateforme** avec 200 pannes de bornes.

**Conséquence en production.** Une panne du réseau de remontée, de Kafka ou de Spark produirait autant d'alertes qu'il y a de bornes
(1 200 chez Lumio), au moment même où l'exploitation a besoin d'un écran lisible.

**Correction proposée (version 1.1).**
1. Si plus de 20 % du parc devient muet **dans la même minute**, ne pas ouvrir d'alerte par borne, mais **un seul incident de plateforme**
   (« coupure de la remontée des mesures »), de gravité critique.
2. Distinguer l'origine du trou : si Kafka n'a reçu **aucun** message d'aucune borne pendant la période, le trou vient de la chaîne, pas des bornes.
3. Ajouter à la recette un scénario « coupure générale » : arrêt du simulateur entier pendant 3 minutes, avec un résultat attendu de 1 incident et 0 alerte par borne.

Ce défaut n'apparaissait pas pendant la recette : il fallait une **interruption globale réelle** pour le révéler. C'est l'intérêt d'une endurance.

## 5. Le lakehouse après 14 heures : compaction

Chaque micro-lot écrit un petit fichier par partition. Résultat de `spark/maintenance.py` :

| Table | Fichiers avant → après | Taille moyenne avant → après |
|---|---|---|
| silver.telemetry | **584 → 3** | 101 Ko → 17 Mo |
| gold.alerts | **472 → 3** | 2,5 Ko → 18 Ko |
| gold.charger_minute | **279 → 2** | 10 Ko → 654 Ko |
| silver.grid_co2 | 19 → 1 | 1,3 Ko → 1,9 Ko |
| bronze.telemetry_raw | 5 fichiers de 13,5 Mo | déjà compactée lors d'une première tentative (voir ci-dessous) |

**Défaut trouvé dans la maintenance elle-même.** La première exécution a compacté bronze, puis échoué sur l'expiration des instantanés :
la procédure Iceberg `expire_snapshots` refuse une expression comme `current_timestamp() - INTERVAL 1 DAYS`
(`number of args and params must match after binding`). Elle exige une valeur fixe. Corrigé : la date limite est calculée en Python.
Le script n'avait jamais tourné sur une table réelle avant cette endurance.

## 6. Chiffres métier de la période

| Indicateur | Valeur |
|---|---|
| Recharges (parc nominal de 200 bornes) | 636 |
| Énergie délivrée | 17,9 MWh |
| CO₂ émis | 780 kg, soit **43,6 g/kWh** en moyenne |
| Intensité carbone RTE reçue | 29 à 48 g/kWh (moyenne 43,8), 28 créneaux de 15 min |

## 7. Conclusions

| Garantie | Verdict |
|---|---|
| Ni perte ni doublon, y compris après les veilles et les relances | ✅ Tenue sur 1,6 million de messages |
| Détection en régime stable | ✅ 84 / 84, p95 12,5 s |
| Reprise automatique après une coupure du canal Python ↔ JVM | ✅ 2 relances sans intervention |
| Maintenance du lakehouse | ✅ Après correction d'un défaut |
| Lisibilité des alertes lors d'une coupure globale | ❌ **Rafale de fausses alertes : correction spécifiée pour la version 1.1** |

**Plateforme arrêtée le 2026-10-28 à 09:19 UTC** (`docker compose stop`). Les volumes sont conservés : `docker compose start` la relance là où
elle s'était arrêtée, grâce aux checkpoints.
