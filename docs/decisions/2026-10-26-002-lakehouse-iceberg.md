# ADR 002 : Lakehouse Apache Iceberg sur S3, catalogue JDBC

- **Date :** 2026-10-26
- **Statut :** Acceptée

## Contexte

Les mesures doivent être conservées (90 jours en brut, sans limite en agrégé) dans un format que la DSI pourra reprendre sur Google Cloud.
Écrire en continu dans de simples fichiers Parquet poserait deux problèmes : un lecteur peut voir un lot à moitié écrit, et une reprise
après panne peut écrire deux fois le même lot.

## Décisions

1. **Apache Iceberg 1.10** : chaque écriture est une **transaction** (un instantané, ou *snapshot*). Un lecteur voit tout ou rien.
   Iceberg permet aussi le voyage dans le temps, l'évolution de schéma, et il est lu par BigQuery (BigLake).
2. **Trois couches** : `bronze.telemetry_raw` (message tel que reçu, avec partition et offset Kafka), `silver.telemetry` (typé, dédoublonné),
   `gold.charger_minute` et `gold.alerts`. Partitionnement **par jour**.
3. **Catalogue JDBC dans PostgreSQL** : la liste des tables et leur instantané courant sont dans la base déjà présente pour la diffusion.
4. **Accès S3 natif d'Iceberg** (`S3FileIO`) vers SeaweedFS, sans la couche Hadoop S3A.
5. **Écritures exactement une fois** : le puits Iceberg natif de Spark l'est par construction (checkpoint + commit). Dans `foreachBatch`, chaque
   ajout est **marqué du numéro de micro-lot** dans l'instantané (`snapshot-property.lumio.batch`) : un lot rejoué après une panne est ignoré.
6. **Maintenance quotidienne** (`spark/maintenance.py`) : suppression des mesures brutes de plus de 90 jours, compaction des petits fichiers,
   expiration des anciens instantanés.

## Alternatives écartées

| Option | Raison du rejet |
|---|---|
| Delta Lake | Équivalent fonctionnellement ; Iceberg est mieux intégré à BigQuery (cible du client) et indépendant d'un éditeur |
| Catalogue Hadoop (fichiers sur S3) | Pas de verrouillage fiable sur un stockage objet : risque de commits concurrents perdus |
| Catalogue REST (service dédié) | Un conteneur de plus, pour un seul écrivain |
| Parquet seul | Pas de transactions : lectures partielles et doublons possibles après une panne |

## Conséquences

- ✅ Lecteurs jamais exposés à un lot partiel ; reprise après panne sans doublon (vérifiée en recette)
- ✅ Sur GCP : même format, même code ; seul le catalogue change (BigLake metastore)
- ⚠️ Le streaming produit beaucoup de petits fichiers : la compaction n'est pas optionnelle
