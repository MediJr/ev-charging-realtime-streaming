# 01 · Brief client : demande initiale

> 🎭 **Scénario fictif.** L'entreprise *Lumio Recharge* et ses interlocuteurs sont inventés pour les besoins du portfolio
> (ce sont les mêmes que dans le [projet 2](https://github.com/MediJr/france-power-mix-incremental-etl), dont cette mission est la suite).
> Les mesures des bornes sont **simulées** ; l'intensité carbone est la donnée **réelle** de RTE (éCO2mix temps réel).
> Ce projet n'est ni commandé, ni validé, ni affilié à RTE.

| | |
|---|---|
| **Date de réception** | 2026-10-26 |
| **Canal** | Email |
| **Client** | Lumio Recharge, opérateur de 1 200 bornes de recharge (client de la mission LUM-2026-001) |
| **Interlocuteurs** | Sophie Bernard, directrice des opérations · Karim Haddad, responsable de l'algorithme de pilotage |
| **Objet** | « Savoir en quelques secondes qu'une borne ne va pas bien » |

---

## Message reçu

> Bonjour,
>
> La base CO₂ que vous nous avez livrée fonctionne très bien. Nous passons maintenant à l'étape suivante.
>
> Aujourd'hui, nous apprenons qu'une borne est en panne **quand un client nous appelle**, parfois des heures plus tard.
> Nos bornes envoient pourtant leur puissance en continu. Nous voudrions :
> - **détecter en quelques secondes** une borne qui ne répond plus, qui consomme anormalement, ou qui reste bloquée ;
> - afficher **en direct** le CO₂ de chaque recharge en cours, avec la donnée RTE ;
> - un **écran de supervision** pour notre centre d'exploitation.
>
> Notre DSI prépare un passage sur **Google Cloud** l'an prochain : la solution doit pouvoir y être déployée sans tout réécrire.
> Pour l'instant, nous n'avons pas de budget cloud : il faut une démonstration sur un poste.
>
> Merci d'avance,
> Sophie Bernard, Lumio Recharge

---

## 🔍 Première analyse (interne, avant la réunion de cadrage)

**Ce que le client exprime :** passer d'une base mise à jour toutes les heures à une **supervision en temps réel**.

**Ce que cela implique techniquement :**
1. **« En quelques secondes »** : le traitement par lots (horaire) ne suffit plus. Il faut du **streaming** : un journal d'événements
   (Kafka) et un moteur qui traite en continu (Spark Structured Streaming).
2. **« Une borne qui ne répond plus »** : on doit détecter une **absence** d'événement, pas seulement un événement anormal.
   C'est plus difficile : il faut raisonner en fenêtres de temps.
3. **Des mesures envoyées par le réseau** : elles peuvent arriver **en retard, en double ou dans le désordre**. Le temps de
   l'événement (*event time*) n'est pas le temps de traitement (*processing time*).
4. **« Sans tout réécrire sur Google Cloud »** : formats ouverts (Parquet, Iceberg), stockage S3, et infrastructure décrite en code
   (**Terraform**).
5. **Une panne du traitement ne doit rien perdre ni rien compter deux fois** : points de reprise (*checkpoints*) et écritures idempotentes.

**Points à clarifier en cadrage :**
- Quel délai de détection est acceptable ? Quelles anomalies, avec quels seuils ?
- Que faire d'une mesure qui arrive avec plusieurs minutes de retard ?
- Combien de temps garder les mesures brutes ?
- Qui regarde l'écran de supervision, et que doit-il y voir en premier ?

➡️ Suite : [02 · Compte rendu de la réunion de cadrage](2026-10-26-02-compte-rendu-cadrage.md)
