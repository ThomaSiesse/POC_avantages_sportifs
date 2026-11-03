# POC Avantages Sportifs

Pipeline de données qui calcule les avantages accordés aux salariés sportifs. Il contrôle aussi les données déclarées et félicite les salariés sur Slack en temps réel.

Les deux avantages viennent de la note de cadrage :

- **Prime sportive** : 5 % du salaire annuel brut pour les salariés qui viennent au travail de façon sportive (marche, course, vélo, trottinette…). Le déplacement doit être cohérent avec la distance domicile–entreprise : 15 km maximum à pied ou en courant, 25 km maximum à vélo ou en trottinette.
- **5 journées bien-être** pour les salariés qui ont fait au moins 15 activités sportives sur les 12 derniers mois.

## Architecture

```
PostgreSQL source ──► Debezium ──► Kafka ─┬─► consumer-slack ──► Slack
   ▲                                       │        └─► monitoring.slack_messages
   │ generator                             │
                                           └─► consumer-minio ──► MinIO (Delta Lake, raw)
                                                                        │ dbt build toutes les 10 min
                                                                        ▼
            Tableau ◄── exports CSV ◄── PostgreSQL DWH ◄────── dbt-duckdb
                                         (marts + monitoring)   (+ Parquet dans MinIO clean)

            Grafana ──► PostgreSQL source + PostgreSQL DWH
```

| Étape | Outil | Rôle |
|---|---|---|
| Source | PostgreSQL 16 + `generator` | Le générateur crée 12 mois d'activités au format Strava, puis une nouvelle activité par minute. |
| Capture | Debezium (Kafka Connect) | Lit le journal de la base (CDC) et publie chaque changement dans le topic `sport.public.activites`. |
| Transport | Kafka 3.8 (KRaft) | Découple la source des consommateurs et permet de relire l'historique. |
| Notification | `consumer-slack` | Filtre les nouvelles activités cohérentes et publie un message de félicitations via un webhook. |
| Stockage brut | `consumer-minio` + MinIO | Écrit les événements par lots dans une table Delta Lake `s3://raw/activites`, en ajout seul. |
| Transformation | dbt-duckdb | Applique les couches staging, intermediate, marts et monitoring, puis calcule les distances via SerpApi. |
| Restitution | PostgreSQL DWH + Tableau | Les marts sont écrites dans le schéma `marts`, puis exportées en CSV pour Tableau Public. |
| Supervision | Grafana 11 | Suit la fraîcheur, la volumétrie, la qualité et les notifications Slack. |

## Prérequis

- Docker et Docker Compose.
- Une clé [SerpApi](https://serpapi.com/) pour les distances domicile–entreprise.
- Un webhook Slack (facultatif : sans lui, le consumer Slack tourne en simulation et écrit les messages dans ses logs).
- Les fichiers `data/donnees_rh.xlsx` et `data/donnees_sportive.xlsx`.

## Installation

1. Crée un fichier `.env` à la racine. Il n'est pas versionné ; mets-y tes propres valeurs :

   ```dotenv
   SOURCE_DB_USER=
   SOURCE_DB_PASSWORD=
   SOURCE_DB_NAME=
   KAFKA_TOPIC=sport.public.activites
   SLACK_WEBHOOK_URL=
   MINIO_ROOT_USER=
   MINIO_ROOT_PASSWORD=
   DWH_USER=
   DWH_PASSWORD=
   DWH_DB=
   SERPAPI_API_KEY=
   GF_SECURITY_ADMIN_USER=
   GF_SECURITY_ADMIN_PASSWORD=
   ```

2. Lance la stack :

   ```bash
   docker compose up -d --build
   ```

Au premier démarrage, voici ce qui se passe :

- le générateur remplit l'historique ;
- `kafka-connect-init` enregistre le connecteur Debezium ;
- `minio-init` crée les buckets ;
- `consumer-minio` lance un premier `dbt build`.

## Accès

| Service | Adresse |
|---|---|
| PostgreSQL source | `localhost:5435` |
| PostgreSQL DWH | `localhost:5436` |
| Kafka | `localhost:29092` |
| Kafka Connect (API) | http://localhost:8083 |
| Console MinIO | http://localhost:9001 |
| Grafana | http://localhost:3000 (dashboard « Pipeline Sport Data ») |

## Transformations dbt

```
dbt/
├── seeds/          règles modifiables sans toucher au SQL
│   ├── parametres_avantages.csv   taux de prime, jours bien-être, seuil d'activités
│   ├── regles_sports.csv          vitesse maximale réaliste par sport
│   └── regles_deplacement.csv     distance maximale par mode de déplacement
├── models/
│   ├── staging/       lecture du Delta et des Excel, typage, dédoublonnage
│   ├── intermediate/  contrôles des activités, distances SerpApi, contrôle des déplacements
│   ├── marts/         dim_salaries, fct_activites, mart_avantages_salaries → DWH
│   └── monitoring/    mon_etat_pipeline, mon_rejets_activites → DWH (lu par Grafana)
└── macros/            schémas personnalisés, export Parquet vers MinIO
```

Contrôles de qualité :

- **Activités** : une activité est rejetée avec un motif dans les cas suivants :
  - salarié inconnu ;
  - fin avant le début ;
  - durée de plus de 12 h ;
  - date dans le futur ou de plus d'un an ;
  - distance nulle ou négative ;
  - vitesse irréaliste pour le sport.
- **Déplacements** : statut `ok`, `incoherent` (distance au-delà du seuil) ou `non_calcule` (adresse introuvable). Un déplacement incohérent n'ouvre pas droit à la prime. Un statut `non_calcule` est marqué « à vérifier ».
- **Tests dbt** : unicité, valeurs non nulles et relations, à chaque build.

Les distances sont calculées dans un modèle Python incrémental. Chaque couple salarié / adresse / mode n'est appelé qu'une fois, pour limiter la consommation de crédits SerpApi.

Pour lancer un build à la main :

```bash
docker exec consumer-minio dbt build --no-use-colors
```

## Modifier le taux de prime

Le taux et les autres paramètres sont dans [dbt/seeds/parametres_avantages.csv](dbt/seeds/parametres_avantages.csv) :

```csv
taux_prime_sportive,nb_jours_bien_etre,nb_activites_min_bien_etre,nb_jours_travailles_an
0.05,5,15,218
```

Le nouveau taux est pris en compte au prochain build, automatique ou manuel. Les marts sont reconstruites entièrement à partir des données brutes, donc **tout l'historique est recalculé**. Chaque build enregistre aussi le taux appliqué dans `monitoring.mon_etat_pipeline`.

## Reporting Tableau

Tableau Public ne se connecte qu'à des fichiers locaux. Ce script exporte toutes les tables du schéma `marts` en CSV :

```bash
./scripts/export_tableau.sh                # → exports/tableau/
./scripts/export_tableau.sh /autre/dossier
```

Dans Tableau, connecte-toi en *Fichier texte* à `dim_salaries.csv`, ajoute les deux autres fichiers et relie-les sur `id_salarie`. Après un nouvel export, fais *Données → Actualiser*. Le dossier `exports/` est exclu de Git, car il contient des salaires.

## Monitoring

Le dashboard Grafana « Pipeline Sport Data » (dossier « Sport Data ») est provisionné automatiquement. Il comporte 4 sections :

- **État du pipeline** : dernière activité en source, dernière ingestion MinIO, dernier build dbt, retard source → DWH.
- **Volumétrie** : activités par couche et évolution dans le temps.
- **Qualité** : rejets par motif, déplacements incohérents ou non calculés, coût total des avantages.
- **Notifications Slack** : messages envoyés, filtrés, en erreur.

## Démo : insérer une nouvelle course

```bash
docker exec -i postgres-source sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
insert into activites (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire)
values (81676, now() - interval '46 minutes', 'Runing', 10800, now(), 'Sortie au bord du Lez');
SQL
```

1. Le message apparaît sur Slack en quelques secondes. Sans webhook, il est visible dans `docker logs consumer-slack`.
2. Attends environ 30 s, le temps que le lot soit écrit dans MinIO.
3. Lance `dbt build` puis `./scripts/export_tableau.sh` pour voir la course dans Tableau.

## Arborescence

```
.
├── docker-compose.yml
├── generator/          générateur d'activités (Python)
├── postgres-source/    schéma de la base source
├── kafka-connect/      configuration du connecteur Debezium
├── consumers/
│   ├── slack/          consumer Kafka → Slack
│   └── minio/          consumer Kafka → Delta Lake + lancement de dbt
├── minio_init/         création des buckets
├── dbt/                projet dbt-duckdb
├── grafana/            datasources et dashboard provisionnés
├── scripts/            export CSV pour Tableau
├── presentation/       support de soutenance et notes
└── data/               fichiers RH et sportifs (non versionnés)
```

## Pistes pour la production

- **Migration AWS** : Kafka → Amazon MSK, MinIO → S3, PostgreSQL → RDS. Le projet dbt reste le même.
- **Orchestration** : lancer dbt depuis un orchestrateur (Airflow) plutôt que depuis le consumer MinIO.
- **Robustesse** :
  - passer Kafka à au moins 3 brokers ;
  - envoyer les alertes Grafana sur Slack ;
  - gérer les secrets dans AWS Secrets Manager.
- **BI** : brancher Tableau Cloud ou Power BI directement sur le DWH, avec un accès restreint.
