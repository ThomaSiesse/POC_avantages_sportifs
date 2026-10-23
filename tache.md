# Projet_12: POC Avantages_Sportifs

## 15/10/2026

### Projet:
1/ création infrastructure de données; Réfléchir à la contruction du projet avec bdd solide

2/ Créer des données automatiquement de même type que l'API strava: https://developers.strava.com/docs/reference/ sur les 12 derniers mois (métadonnées: ID ; ID salarié ; Date de début de l'activité ; Type ; Distance (en mètres, à laisser vide
si non pertinent – par exemple pour l'escalade) ; Date de fin de l'activité ;
Commentaire.)
et générer des messages Slacks pour chaques activités

3/ Test sur les données: cohérence et fonctionnalité avec GreatExpectation ou SODA

4/ pipeline ETL

5/ Monitoring des données

6/ interface PowerBI: affichage KPI important

+ info: https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Note+de+cadrage+_+POC+Avantages+Sportifs+(1).pdf

Liberté techno --> voir faisabilité avec AWS

data: 
    - Données RH (info perso + mode déplacement: vérifier si le mode de déplacement est correct)
    - Données Sportives (quel sport pratiqué)
    - Données Activitées à créer

** Infrastructure**
1/ généré des datas:
    -Script python qui génère des datas pour les 12 derniers mois + création de data automatique chaque jours en supprimant celle de plus de 1 an (ex: projet 9 generator ticket) script à lancer chaque jour pour faire évoluer les datas ou alors le rendre automatique quand un jours passe mais peut être compliqué pour rien car la finalité c'est connecté à une API stack: kafka
2/ lire en streaming les data: workflow (ex: projet Modélisez une infra dans le cloud) stack spark + redpanda
3.1/ envoi message SLACK via script python
3.2/ envoi data dans BDD (ex: projet infra de données 50h) stack: PostgreSQL + dbt
4/ transform data (ex: projet construire infra données) stack:dbt pour faire l'ETL
5/ save data transformé stack: dbt + PostgreSQL
6/PowerBI

Ordre de travail

1/ Docker local : Kafka, PostgreSQL, script Python, consommateur qui envoie sur Slack.
2/ dbt-duckdb pour les transformations, écriture en Delta Lake (MinIO en local).
3/ Une couche de restitution pour Power BI (PostgreSQL ou autre).
4/ Migration vers AWS une fois la chaîne validée.

### A faire:
    - schéma d'infrastructure ok

## 16/10/2026
### A faire:
    - environnement docker via docker-compose
    - avec mise en place envirronement KAFKA, PostgreSQL MinIO + 3 scripts python:  1 génétateur
            Consommateur slack
            Consommateur delta
    - 1 fichier Mot de passe
    - debezium qqui capte les changement dans la bdd postgre
    - dbt core
    - utilyser truvy (open source) pour voir les défault de sécurité
    - plusieurs conteneurs
    - RDS 1 conteneur
    - kafka
    - analytics 1 conteneur
    - kafka: filtrage cas exceptionnel, si en dehors de bornes
    - consumers slack avec if / else 

### Fait
    - docker-compose.yaml
    - dossier dbt / postgres-source / producer
    - .env configuré
    - .gitignore

arborescence:
Projet_12/
├── docker-compose.yaml                ✅
├── .env                               ✅ 
├── .gitignore 
├── README.md                          ✅ 
│
├── data/ 
│   ├──minio/
│   |   ├──.minio.sys/                 ✅
│   |   ├── clean/                     ✅
│   |   └── raw/                       ✅ 
│   ├── donnees_rh.xlsx                ✅
│   └── donnees_sportive.xlsx          ✅ 
│
├── postgres-source/init/              ✅ 
│   └── 01_tables.sql                  ✅
│
├── kafka-connect/
|   └──postgres-source.json            ✅
|
├── generator/                         ✅
│   ├── Dockerfile                     ✅
│   ├── requirements.txt               ✅ psycopg, pandas, openpyxl
│   └── generator.py                   ✅
│
├── producer/                          ✅ build: ./producer (LISTEN → Kafka)
│   ├── Dockerfile                     ❌
│   ├── requirements.txt               ❌ psycopg, confluent-kafka
│   └── producer.py                    ❌
│
├── consumers/
│   ├── slack/                         ✅ 
│   │   ├── Dockerfile                 ✅
│   │   ├── requirements.txt           ✅ 
│   │   └── consumer_slack.py          ❌ filtre + envoi au webhook
│   └── minio/                         ✅ 
│       ├── Dockerfile                 ✅
│       ├── requirements.txt           ✅ confluent-kafka, boto3, dbt-duckdb
│       └── consumer_minio.py          ❌ écrit dans le bucket raw + lance dbt
│
└── dbt/                               ✅ monté dans consumer-minio (/dbt)
    ├── dbt_project.yml                ❌
    ├── profiles.yml                   ❌ profil duckdb (S3 MinIO + attach postgres-dwh)
    └── models/
        ├── bronze/                    ❌ lecture du raw dans MinIO
        ├── silver/                    ❌ nettoyage
        └── gold/                      ❌ marts → MinIO clean + postgres-dwh

## 19/10/2026
### A faire
    - MAJ pour utiliser Debezium ok
    - generator.py

### cette après midi:
    - ipykernel pour lire les données rh
    - voir si un id salarié eszt renseigné pour plusieur sport
    - définir les sport sans distance par rapport à la liste de sport pratiqué
    - continué le generator de données

### Demain
    - géré les activités avec distance et sans du generator
    - le defaut duration
    - d'autres commentaire
    - comprendre les fonctions et pourquoi
    - pourquoi date embauche/naissance
    - comprendre backfile
    - insert live activities, obligé?
    - cur?

## 21/10/2026
### Fait:
    - géré les activités avec distance et sans du generator
    - le defaut duration

## 22/10/2026
### Tâche:
    - mkdir consumers/slack/ Dockerfile, requirement.txt, consumers.py

CONNECTER MINIO ET LANCER LE DOCKER COMPOSE
    - docker compose ok


docker ok
generator ok

A faire: Script python consumer_minio/ consumer_slack
    Traitement données avec dbt
    écriture dbt dans postgres-dwh
    connexion power bi