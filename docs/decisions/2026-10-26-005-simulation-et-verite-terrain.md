# ADR 005 : Simulateur avec défauts injectés et vérité terrain

- **Date :** 2026-10-26
- **Statut :** Acceptée

## Contexte

Le client ne peut pas brancher ses vraies bornes pour une démonstration. Un simulateur « parfait » ne prouverait rien : la difficulté du
streaming vient justement des pannes, des retards et des doublons.

## Décisions

1. **200 bornes sur 12 sites** (deux tiers en 22 kW, un tiers en 50 kW), une mesure toutes les 5 s, sessions de recharge réalistes.
   Même graine aléatoire (`SIM_SEED`) = même parc.
2. **Défauts injectés volontairement** :

   | Défaut | Fréquence | Ce qu'il éprouve |
   |---|---|---|
   | Borne muette (2 à 4 min) | Les trois anomalies ensemble : environ 0,3 par borne et par heure, soit près d'une par minute sur le parc (une surpuissance tirée sur une borne libre est abandonnée) | Détection d'une absence |
   | Surpuissance (+15 à +40 %) | | Règle de seuil |
   | Borne bloquée (session à 0 kW, 3 à 5 min) | | Règle de durée |
   | Message en double | 1 % | Dédoublonnage |
   | Retard de 5 à 40 s | 1 % | Temps d'événement |
   | Retard de 3 à 5 min | 0,2 % | Watermark (écarté et compté) |

3. **Vérité terrain** : chaque injection est publiée dans `sim.injections`. La recette compare ce qui a été injecté à ce qui a été détecté
   (vue `serving.v_injection_detection`) : taux de détection et délai, par type d'anomalie.
4. **Intensité carbone réelle** : le collecteur lit l'API éCO2mix temps réel de RTE, la même source qu'au projet 2.

## Conséquences

- ✅ Chaque garantie du pipeline est éprouvée par un défaut réel, et mesurée
- ⚠️ La vérité terrain est de l'instrumentation : en production, il n'y en a pas. On mesurerait alors le délai entre l'heure de la mesure
  et l'heure de l'alerte, sans savoir quelles anomalies ont été manquées
