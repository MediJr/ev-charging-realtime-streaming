# ADR 001 : Streaming avec Redpanda (API Kafka) et Spark Structured Streaming

- **Date :** 2026-10-26
- **Statut :** Acceptée

## Contexte

Le client veut détecter une anomalie de borne en moins de 30 s. La base horaire du projet 2 ne suffit plus : il faut un traitement
**continu**. Les messages des bornes passent par un réseau 4G : ils peuvent arriver **en retard, en double ou dans le désordre**.

## Décisions

1. **Journal d'événements compatible Kafka : Redpanda.** Les producteurs écrivent, les consommateurs lisent à leur rythme, et l'historique
   est rejouable (rétention 7 jours). Redpanda parle le protocole Kafka : le code ne change pas avec un Kafka managé sur le cloud.
2. **Un topic par type de message**, clé = `charger_id` pour la télémétrie : l'ordre est garanti pour une même borne, et les 6 partitions
   répartissent la charge. `grid.co2` est **compacté** (seule la dernière valeur d'un créneau est gardée).
3. **Spark Structured Streaming 4.0** en **temps d'événement** :
   - **watermark de 2 minutes** : Spark attend les retardataires 2 min, puis les écarte et les compte ;
   - **dédoublonnage** par `event_id` (`dropDuplicatesWithinWatermark`) ;
   - **état par borne** (`applyInPandasWithState`) pour les règles d'anomalie, avec un **délai de traitement** pour détecter
     l'**absence** de message (borne muette).
4. **Règles d'anomalie en Python pur** (`lumio_stream.monitor`), appelées par Spark : elles se testent sans Spark, en quelques millisecondes.
5. **Spark en mode local** dans un conteneur (4 cœurs, 1,8 Go) : suffisant pour 200 bornes, sans cluster à administrer.

## Alternatives écartées

| Option | Raison du rejet |
|---|---|
| Apache Kafka (avec KRaft) | Plus lourd en mémoire sur un poste ; même protocole, donc remplaçable à tout moment |
| Apache Flink | Excellent pour le streaming pur, mais un second moteur à apprendre ; Spark sert aussi aux lots et à la maintenance Iceberg |
| Spark Streaming en temps de traitement | Une mesure en retard serait rangée dans la mauvaise minute, et les doublons comptés deux fois |
| Détecter la borne muette avec une requête planifiée | Ajoute une seconde mécanique ; l'état Spark avec délai fait la même chose dans le flux |

## Complément du 2026-10-27 : filet de sécurité du silence

En recette, 3 bornes muettes ont été **manquées** pendant la résorption d'un retard : un délai en *temps de traitement* ne se déclenche
pas quand Spark traverse plusieurs minutes de données en quelques secondes. Un trou de plus de 60 s entre deux mesures est désormais
aussi vérifié en *temps d'événement*, au retour de la borne : l'alerte arrive en retard, mais n'est jamais perdue.
Vérifié en conditions réelles (Spark arrêté 4 min, borne muette 169 s détectée au rattrapage).

## Conséquences

- ✅ Détection en quelques secondes (p95 11,6 s en recette), retards et doublons traités, état tolérant aux pannes (checkpoints)
- ⚠️ Le moniteur, une fonction Python par borne, est le goulot d'étranglement : environ 1 440 messages/s sur un poste
- ⚠️ Un état avec délai de traitement fait tourner des micro-lots « sans données » pour surveiller les délais : un test qui attend la fin
  du flux (`availableNow`) ne se termine jamais. Le test du moniteur arrête donc la requête lui-même
