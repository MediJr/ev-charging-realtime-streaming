# Changelog

Toutes les évolutions notables du projet sont consignées ici.
Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/). Le projet suit le [versionnage sémantique](https://semver.org/lang/fr/).

## [1.0.0] - 2026-10-27

Première version livrée au client.

### Ajouté
- Dossier de mission LUM-2026-002 : brief, cadrage, cahier des charges
- Plateforme Docker Compose : Redpanda (API Kafka) et sa console, SeaweedFS (S3), PostgreSQL, Spark 4.0, simulateur, collecteur CO₂, Grafana
- Simulateur de 200 bornes avec anomalies et défauts réseau injectés, et vérité terrain (`sim.injections`)
- Collecteur de l'intensité carbone réelle de RTE (éCO2mix temps réel), topic compacté
- Spark Structured Streaming : 6 requêtes, temps d'événement, watermark de 2 min, dédoublonnage, état par borne
  (borne muette, surpuissance, borne bloquée), minutes et CO₂, suivi des micro-lots
- Lakehouse Iceberg 1.10 (bronze, silver, gold), catalogue JDBC, écritures exactement une fois, script de maintenance
- Base de diffusion et tableau Grafana de 13 panneaux, provisionnés en code
- Infrastructure GCP en Terraform (Cloud Storage, BigQuery, BigLake, comptes de service, alerte de budget), validée
- 22 tests (dont 5 Spark de bout en bout), intégration continue en 3 jobs, script de recette, graphiques FR/EN, rapports LaTeX

### Corrigé (pendant le développement et la recette)
- Colonne de dates entièrement vide refusée par Arrow (lot sans alerte)
- Tests Spark ignorés sans le dire (`pyspark` absent du chemin Python de l'image) ; workers sans `lumio_stream`
- Simulateur : attente négative (`sleep length must be non-negative`)
- `event_id` tirés d'une graine fixe, donc répétés après un redémarrage : UUID v4 du système
- Rapprochement de recette faussé par la lecture de bronze et silver à des instants différents : ensemble fixe
- Bornes muettes manquées pendant un rattrapage : filet de sécurité en temps d'événement
