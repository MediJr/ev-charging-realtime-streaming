# ADR 003 : Diffusion dans PostgreSQL, supervision avec Grafana

- **Date :** 2026-10-26
- **Statut :** Acceptée

## Contexte

L'écran du centre d'exploitation doit se rafraîchir toutes les 5 secondes et afficher d'abord les alertes ouvertes. Interroger le
lakehouse à chaque rafraîchissement serait lent (lecture de fichiers sur S3) et coûteux sur le cloud.

## Décisions

1. **Une base de diffusion PostgreSQL** (schéma `serving`), alimentée par Spark dans `foreachBatch` :
   état courant de chaque borne, alertes (une ligne par épisode, ouverte puis résolue), minutes closes, intensité CO₂, progression des micro-lots.
2. **Toutes les écritures sont des UPSERT** sur une clé naturelle : un micro-lot rejoué ne crée aucun doublon, et ne change pas l'heure
   de détection d'une alerte déjà enregistrée (`ON CONFLICT DO NOTHING`).
3. **Grafana provisionné en code** : source de données et tableau de bord sont des fichiers versionnés, chargés au démarrage.
   Accès anonyme en lecture seule pour l'écran du centre d'exploitation.
4. **La progression de chaque micro-lot** (débit, durée, lignes écartées par la watermark) est enregistrée par un `StreamingQueryListener` :
   la santé du flux se lit dans le même écran que les bornes.
5. **Les alertes sont aussi publiées dans Kafka** (`charger.alerts`), pour de futurs consommateurs (SMS, ticketing).

## Conséquences

- ✅ Rafraîchissement en quelques millisecondes ; l'opérateur voit les alertes, le débit et la fraîcheur au même endroit
- ⚠️ La publication Kafka des alertes est « au moins une fois » : un consommateur doit dédoublonner sur (borne, type, heure de l'anomalie)
- ⚠️ Sur GCP, la diffusion passerait par BigQuery (plus lent à interroger) ou par Cloud SQL
