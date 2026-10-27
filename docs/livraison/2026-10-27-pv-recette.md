# Procès-verbal de recette

> 🎭 **Scénario fictif** : voir le [brief client](../mission/2026-10-26-01-brief-client.md).

| | |
|---|---|
| **Référence** | LUM-2026-002 |
| **Version livrée** | 1.0.0 |
| **Date** | 2026-10-27 |
| **Environnement** | Plateforme vierge (volumes Docker supprimés), 200 bornes simulées, intensité CO₂ réelle de RTE |
| **Participants** | Sophie Bernard, Karim Haddad (Lumio Recharge), le data engineer |

## Déroulé

| Heure (UTC) | Étape |
|---|---|
| 19:12:40 | `docker compose up -d` sur une plateforme vierge |
| 19:12 → 19:32 | Fonctionnement nominal : environ 40 messages/s, défauts injectés par le simulateur |
| 19:32:49 | **Arrêt brutal de Spark** (`SIGKILL`) |
| 19:33:41 | Relance ; premier micro-lot à 19:33:59 (n° 41, juste après le 40 validé avant l'arrêt) |
| 19:47 | Contrôles de rapprochement (`spark/recette.py`) |
| 19:53 → 19:58 | **Test de charge** : 3 000 bornes de plus, à 1 message/s (≈ 2 960 messages/s) |
| 21:01 → 21:05 | Vérification du filet de sécurité du silence : Spark arrêté 4 min, puis rattrapage |

## Critères d'acceptation

| # | Critère | Résultat | Preuve |
|---|---|---|---|
| C1 | La plateforme démarre avec `docker compose up` sur un poste neuf | ✅ | 9 services démarrés, topics créés, Grafana alimenté en moins d'une minute |
| C2 | Les trois anomalies injectées sont toutes détectées | ✅ | **27 / 27** en régime nominal (10 surpuissances, 11 bornes muettes, 6 bornes bloquées), **0 fausse alerte** |
| C3 | Délai de détection < 30 s au 95e centile | ✅ | **p95 11,6 s** (médiane 5,9 s, maximum 13,9 s) |
| C4 | Doublons comptés une fois ; retardataires comptés à part | ✅ | 83 292 messages = 82 347 mesures + 808 doublons + 137 retardataires ; 808 doublons = 808 injectés ; 137 / 137 absents expliqués par un retard injecté |
| C5 | Arrêt brutal de Spark : ni perte, ni doublon | ✅ | Reprise au micro-lot suivant ; 0 message Kafka lu deux fois ; 0 doublon dans silver ; 3 654 messages rattrapés en 8,4 s |
| C6 | Le CO₂ des recharges utilise l'intensité RTE réelle | ✅ | 186 recharges, 4,8 MWh, 211 kg de CO₂ à 44 g/kWh (intensité RTE de la soirée) |
| C7 | `terraform validate`, tests et intégration continue verts | ✅ | `fmt`, `init -backend=false`, `validate` OK ; 22 tests (dont 5 Spark de bout en bout) ; CI à vérifier après publication |
| C8 | Aucun secret versionné | ✅ | `.env`, `terraform.tfvars` et l'état Terraform exclus ; identités S3 générées au démarrage |

## Test de charge (question Q4)

| Mesure | Résultat |
|---|---|
| Débit envoyé | ≈ 2 960 messages/s (74 fois le débit nominal) |
| Bronze, silver | Suivent : jusqu'à 5 700 et 4 600 messages/s |
| Moniteur à état | **Goulot d'étranglement** : ≈ 1 440 messages/s ; délai de détection à 130-160 s (p95) pendant la surcharge |
| Stabilité | Aucun plantage ; mémoire de Spark à sa limite (2,44 Go) ; retard résorbé 10 min après la fin du test |
| Capacité | ≈ 1 440 messages/s, soit ≈ 7 000 bornes à une mesure toutes les 5 s (Lumio en exploite 1 200) |

## Réserves

1. **Bornes muettes pendant un rattrapage** : 3 manquées pendant la résorption du retard du test de charge. Corrigé (filet de sécurité
   en temps d'événement), couvert par un test, et vérifié en conditions réelles. L'alerte est alors **tardive**, jamais perdue.
2. **Capacité du moniteur** : au-delà d'environ 1 440 messages/s, il faut un cluster Spark (Dataproc) et plus de partitions.
3. **Relance après arrêt manuel** : un `docker kill` n'est pas relancé par la politique `unless-stopped` ; un vrai plantage l'est
   (4 relances automatiques observées pendant 31 h d'endurance, après des mises en veille du poste).

## Décision

**Recette prononcée avec les réserves 2 et 3, non bloquantes pour un parc de 1 200 bornes.**

| | Client | Prestataire |
|---|---|---|
| **Nom** | Sophie Bernard | Data engineer |
| **Date** | 2026-10-27 | 2026-10-27 |
| **Mention** | Bon pour réception | Livré |
