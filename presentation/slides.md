# Pitch deck — POC Avantages Sportifs

## Consignes pour générer le PowerPoint

- **Rôle** : agis comme un data engineer qui présente son POC.
- **Format** : 10 slides, 16:9, structure PSI (Problème → Solution → Impact).
- **Audience** : board d'une ETI de 20 personnes, 20 minutes maximum.
- **Contraintes** : 5 bullets maximum par slide, 10 mots maximum par bullet, aucune statistique inventée.
- **Ton** : professionnel, confiant, orienté action.
- **Style** : simple et élégant.
  - Fond blanc, texte bleu nuit `#1F2A44`, une seule couleur d'accent vert `#2BB673`.
  - Une police sans empattement, par exemple Montserrat pour les titres et Inter pour le texte.
  - Beaucoup d'espace vide, une idée par slide, pas d'images décoratives.
  - Un bandeau discret en haut de chaque slide indique la partie : PROBLÈME, SOLUTION ou IMPACT.
  - Les notes de l'orateur sont dans `notes_orateur.md`, à placer dans les notes de chaque slide.

---

## Slide 1 — Titre

**Avantages Sportifs**

Récompenser le sport, piloter les coûts, en temps réel

*POC data — Thomas SIESSE, Data Engineer — novembre 2026*

---

## Slide 2 — PROBLÈME · Des avantages promis, impossibles à piloter

- Prime sportive : 5 % du salaire annuel brut
- Cinq journées bien-être dès 15 activités par an
- Modes de déplacement déclarés, jamais vérifiés
- Activités sportives dispersées, sans suivi fiable
- Coût réel inconnu, paramètres susceptibles d'évoluer

---

## Slide 3 — PROBLÈME · Cinq défis techniques

- Capter chaque activité dès sa saisie
- Contrôler la cohérence des données entrantes
- Vérifier la distance domicile–entreprise
- Recalculer l'historique quand un paramètre change
- Superviser le pipeline de bout en bout

---

## Slide 4 — SOLUTION · Une architecture événementielle open source

**Schéma (de gauche à droite) :**

```
PostgreSQL source ──► Debezium ──► Kafka ─┬─► Consumer Slack ──► Slack
                                           │
                                           └─► Consumer MinIO ──► MinIO (Delta Lake)
                                                                        │
                                                                        ▼
                                           Tableau ◄── PostgreSQL DWH ◄── dbt-duckdb

                      Grafana surveille la source, le DWH et Slack
```

- Debezium capte chaque changement de la base source
- Kafka diffuse les événements à deux consommateurs
- MinIO conserve le brut au format Delta Lake
- dbt-duckdb transforme, PostgreSQL DWH restitue
- Toute la chaîne tourne dans Docker Compose

---

## Slide 5 — SOLUTION · Du brut à l'indicateur, en trois couches

- Staging : typage et dédoublonnage des événements
- Intermediate : contrôles métier et distances via SerpApi
- Marts : salariés, activités, avantages par salarié
- Résultats dans PostgreSQL et en Parquet sur MinIO
- Build automatique toutes les dix minutes

---

## Slide 6 — SOLUTION · Des données contrôlées, des erreurs remontées

- Chaque activité rejetée porte un motif explicite
- Durée, vitesse, dates et salarié contrôlés
- Distance maximale : 15 km à pied, 25 km à vélo
- Tests dbt à chaque build : unicité, nullité, relations
- Règles métier dans des fichiers CSV modifiables

---

## Slide 7 — SOLUTION · Un pipeline qui se surveille

- Grafana : fraîcheur, volumes, rejets, coûts
- Un état du pipeline enregistré à chaque build
- Chaque message Slack tracé : envoyé, filtré, erreur
- Seuils colorés pour repérer un retard

---

## Slide 8 — IMPACT · Les indicateurs clés dans Tableau

- Coût total : primes et journées bien-être
- Nombre de journées bien-être accordées
- Pratique sportive et jours de sport par an
- Répartition des salariés par mode de déplacement
- Données exportées du DWH en une commande

---

## Slide 9 — IMPACT · Démonstration en direct

- Saisie d'une nouvelle course dans la base
- Félicitations publiées automatiquement sur Slack
- Taux de prime modifié de 5 % à 7 %
- Indicateurs recalculés dans Tableau et Grafana

---

## Slide 10 — IMPACT · Prêt pour la production

- Taux ajustable sans code, historique recalculé
- Déplacements incohérents signalés avant tout paiement
- Migration AWS : MSK, S3, RDS, transformations inchangées
- Prochaines étapes : orchestration Airflow, alertes, secrets
- Décision attendue : lancer la mise en production
