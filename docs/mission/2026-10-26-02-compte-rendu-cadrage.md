# 02 · Compte rendu de la réunion de cadrage

> 🎭 **Scénario fictif** : voir le [brief client](2026-10-26-01-brief-client.md).

| | |
|---|---|
| **Date** | 2026-10-26 |
| **Format** | Visioconférence, 45 min |
| **Participants** | Sophie Bernard (directrice des opérations) · Karim Haddad (algorithme de pilotage) · Data engineer (prestataire) |

---

## 1. Questions posées et réponses du client

| # | Question du prestataire | Réponse du client |
|---|---|---|
| 1 | Quel délai de détection visez-vous ? | Moins de **30 secondes** entre l'anomalie et l'alerte, dans 95 % des cas. |
| 2 | Quelles anomalies ? | **Borne muette** (plus de message depuis 60 s), **surpuissance** (au-dessus de la puissance nominale de 10 %), **borne bloquée** (session ouverte mais 0 kW pendant plus de 2 minutes). |
| 3 | À quelle fréquence une borne envoie-t-elle sa mesure ? | Toutes les **5 secondes** : puissance instantanée, énergie cumulée, état, session en cours. Notre réseau 4G perd ou retarde parfois des messages, et le boîtier renvoie parfois un message deux fois. |
| 4 | Que faire d'une mesure en retard ? | Si elle arrive dans les **2 minutes**, elle doit compter. Au-delà, on l'écarte, mais il faut savoir combien. |
| 5 | Comment calculer le CO₂ d'une recharge ? | Énergie délivrée × **taux de CO₂ RTE le plus récent** (le même que dans la mission précédente). |
| 6 | Que voit l'opérateur en premier ? | Les **alertes ouvertes**, puis la puissance totale du parc et le CO₂ en cours. |
| 7 | Combien de temps garder les mesures ? | Les mesures brutes **90 jours**, les agrégats à la minute sans limite. |

## 2. Décisions

| # | Sujet | Décision |
|---|---|---|
| D1 | Démonstration | **Simulateur** de 200 bornes (12 sites, 22 kW et 50 kW), avec pannes, retards et doublons injectés **volontairement** |
| D2 | Transport | Un journal d'événements compatible **Kafka** (Redpanda), un topic par type de message |
| D3 | Traitement | **Spark Structured Streaming**, en temps d'événement, avec **watermark** de 2 minutes et dédoublonnage |
| D4 | Stockage | Un **lakehouse Apache Iceberg** sur stockage S3 (format ouvert, lisible par BigQuery) |
| D5 | Supervision | **Grafana**, alimenté par une base PostgreSQL de diffusion |
| D6 | Cloud | Infrastructure GCP décrite en **Terraform**, validée mais **non déployée** (pas de budget cette année) |
| D7 | Exécution | Toute la plateforme dans **Docker Compose** (même choix que la mission e-commerce du prestataire) |

## 3. Questions business retenues

| # | Question | Indicateur |
|---|---|---|
| **Q1** | En combien de temps une anomalie est-elle détectée ? | Délai entre la mesure anormale et l'alerte (médiane, 95e centile) |
| **Q2** | Quelles anomalies, sur quelles bornes, à quelle fréquence ? | Alertes par type, par site, par borne |
| **Q3** | Combien de CO₂ émet chaque recharge, en direct ? | g de CO₂ par session, intensité RTE appliquée |
| **Q4** | La plateforme tient-elle la charge et résiste-t-elle à une panne ? | Débit maximal, reprise après arrêt brutal sans perte ni doublon |

➡️ Suite : [03 · Cahier des charges](2026-10-26-03-cahier-des-charges.md)
