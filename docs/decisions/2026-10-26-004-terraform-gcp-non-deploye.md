# ADR 004 : Infrastructure GCP en Terraform, validée mais non déployée

- **Date :** 2026-10-26
- **Statut :** Acceptée

## Contexte

La DSI de Lumio Recharge prévoit Google Cloud l'an prochain, sans budget cette année. Le client veut savoir que la solution pourra y être
déployée « sans tout réécrire ». Déployer réellement demanderait un compte de facturation et des coûts récurrents.

## Décisions

1. **Infrastructure décrite en Terraform** (`terraform/`), fournisseur `hashicorp/google` 8.8 :
   - bucket Cloud Storage du lakehouse, en région Paris (`europe-west9`), accès public interdit, versionnage, **suppression automatique
     des mesures brutes après 90 jours** et des anciennes versions après 7 jours ;
   - dataset BigQuery des tables gold ;
   - **connexion BigLake** : BigQuery lit les tables Iceberg sans les copier ;
   - **deux comptes de service, au moindre privilège** : le traitement écrit dans le bucket et le dataset ; la supervision lit seulement ;
   - **alerte de budget** à 50, 90 et 100 % (si un compte de facturation est fourni).
2. **Validation sans compte** : `terraform fmt -check`, `terraform init -backend=false` et `terraform validate`, en local et dans la CI.
   Un `terraform plan` demande des identifiants GCP : il sera fait avec la DSI.
3. Le moteur de traitement (Dataproc Serverless) et Kafka managé ne sont **pas** décrits : ce sont les postes les plus coûteux, à
   dimensionner avec la DSI.

## Correspondance local → cloud

| Local (Docker) | Google Cloud |
|---|---|
| SeaweedFS (S3) | Cloud Storage |
| Catalogue Iceberg JDBC | BigLake metastore |
| Spark local | Dataproc Serverless (même code PySpark) |
| Redpanda | Managed Service for Apache Kafka (même protocole) |
| PostgreSQL + Grafana | BigQuery (+ Grafana ou Looker Studio) |

## Conséquences

- ✅ L'infrastructure est relue, versionnée et reproductible avant même d'exister
- ⚠️ `validate` vérifie la syntaxe et la cohérence, pas les droits ni les quotas du projet réel : le premier `plan` reste à faire
