# Notes de l'orateur — POC Avantages Sportifs

Durée cible : environ 15 minutes, démonstration comprise, puis 10 minutes de discussion.

| Slide | Partie | Durée |
|---|---|---|
| 1 | Titre | 0 min 30 |
| 2–3 | Problème | 2 min 30 |
| 4–7 | Solution | 6 min |
| 8 | Reporting | 1 min |
| 9 | Démonstration | 4 min |
| 10 | Impact et suite | 1 min |

> Les chiffres cités pendant la démo viennent du jeu de données de test, généré pour le POC. Les présenter comme tels, jamais comme des chiffres réels de l'entreprise.

---

## Slide 1 — Titre (30 s)

« Bonjour. Je vous présente le POC Avantages Sportifs : une chaîne de données qui calcule les avantages liés au sport, contrôle les déclarations et félicite les salariés en temps réel. Je vais suivre trois temps : le problème, la solution, puis l'impact, avec une démonstration en direct. »

---

## Slide 2 — Des avantages promis, impossibles à piloter (1 min 15)

- La note de cadrage prévoit deux avantages :
  - une prime de 5 % du salaire annuel brut pour les salariés qui viennent au travail de façon sportive ;
  - cinq journées bien-être pour ceux qui pratiquent au moins 15 activités dans l'année.
- Aujourd'hui, le mode de déplacement est seulement déclaré dans le fichier RH. Rien ne vérifie qu'il est plausible.
- Les activités sportives ne sont ni centralisées ni contrôlées.
- Conséquence : on ne connaît ni le coût réel, ni qui y a droit.
- Le taux de 5 % n'est pas gravé dans le marbre. Si on le change, il faut pouvoir recalculer.

Transition : « Derrière ce besoin métier, il y a cinq défis techniques. »

---

## Slide 3 — Cinq défis techniques (1 min 15)

- **Temps réel** : une course doit déclencher un message Slack dans la foulée, pas le lendemain.
- **Qualité** : les données entrantes peuvent être fausses (durée de 20 h, vitesse impossible, date dans le futur).
- **Contrôle des déplacements** : 15 km maximum à pied ou en courant, 25 km à vélo, en trottinette ou autre. Les erreurs doivent être remontées.
- **Paramètres évolutifs** : si le taux ou une donnée source change, tous les indicateurs doivent être recalculés, historique compris.
- **Supervision** : savoir à tout moment si la chaîne fonctionne et si les données sont fraîches.

Transition : « Voici l'architecture que j'ai construite pour y répondre. »

---

## Slide 4 — Architecture (2 min)

Suivre le schéma de gauche à droite.

1. **PostgreSQL source** : la base applicative où arrivent les activités. Un générateur simule un an d'historique au format Strava, puis de nouvelles activités en continu.
2. **Debezium** lit le journal de la base (CDC). On ne modifie pas l'application et on capte chaque insertion, mise à jour ou suppression.
3. **Kafka** sert de bus d'événements. Il découple la source des consommateurs, et chaque consommateur lit à son rythme. On peut rejouer l'historique.
4. **Deux consommateurs Python** :
   - **Slack** filtre les activités et publie les félicitations via un webhook ;
   - **MinIO** écrit le brut par lots dans une table Delta Lake, puis lance dbt.
5. **dbt-duckdb** transforme les données. Les résultats vont dans **PostgreSQL DWH**, lu par **Tableau**.
6. **Grafana** surveille le tout.

Justification globale :
- Tout est open source et tourne dans Docker Compose, donc reproductible sur n'importe quel poste.
- MinIO parle l'API S3, Kafka a son équivalent managé (MSK), PostgreSQL aussi (RDS). La migration vers AWS est prévue dès la conception.

---

## Slide 5 — Trois couches (1 min 30)

Architecture médaillon (bronze, silver, gold) :

- **Brut (bronze)** : la table Delta dans MinIO, en ajout seul, jamais modifiée. C'est notre source de vérité, ce qui permet de tout recalculer.
- **Staging** : lecture du Delta et des deux fichiers Excel (RH et sportif), typage, renommage. Kafka garantit « au moins une fois », donc des doublons sont possibles. On garde la dernière version de chaque activité.
- **Intermediate** : contrôles métier et calcul de la distance domicile–entreprise via SerpApi (Google Maps). Ce modèle est incrémental : une adresse n'est calculée qu'une fois, pour limiter les appels payants.
- **Marts** : trois tables (dim_salaries, fct_activites, mart_avantages_salaries). Elles sont écrites dans PostgreSQL pour le BI, avec une copie Parquet dans MinIO.
- Le consommateur MinIO lance un build toutes les dix minutes.

Pourquoi DuckDB et pas Spark : le volume est celui d'une ETI de 20 personnes. DuckDB lit directement Delta, Excel et S3, sans cluster à maintenir.

---

## Slide 6 — Qualité des données (1 min 30)

- Chaque activité passe par des contrôles explicites :
  - salarié inconnu ;
  - fin avant le début ;
  - durée supérieure à 12 h ;
  - date dans le futur ou de plus d'un an ;
  - distance nulle ou négative ;
  - vitesse irréaliste pour le sport.
- Une activité rejetée n'est pas supprimée : elle garde son motif. On peut donc les compter et les expliquer.
- Déplacements : statut « ok », « incohérent » ou « non calculé » (adresse introuvable).
  - Un déplacement incohérent n'ouvre pas droit à la prime.
  - Un statut « non calculé » est signalé « à vérifier ».
- Des tests dbt tournent à chaque build (unicité, valeurs non nulles, relations entre tables). Un test en échec bloque les modèles qui en dépendent.
- Les seuils (vitesse maximale par sport, distance maximale par mode) sont dans des fichiers CSV. Le métier peut les modifier sans toucher au SQL.

---

## Slide 7 — Monitoring (1 min)

- Un tableau de bord Grafana en quatre parties :
  - état du pipeline (fraîcheur) ;
  - volumétrie ;
  - qualité (rejets par motif, déplacements incohérents, coût total) ;
  - notifications Slack.
- À chaque build, dbt ajoute une ligne d'état dans `monitoring.mon_etat_pipeline`. On garde ainsi l'historique des volumes et du coût, avec le taux de prime appliqué à chaque calcul.
- Le consommateur Slack trace chaque activité traitée : envoyée, filtrée, rejetée ou en erreur.
- Des seuils de couleur signalent visuellement un retard ou une hausse des rejets.

---

## Slide 8 — Reporting Tableau (1 min)

- Indicateurs demandés :
  - coût des avantages (primes et journées bien-être) ;
  - nombre de journées bien-être ;
  - pratique sportive et jours de sport par an ;
  - mode de déplacement.
- Pourquoi Tableau et pas Power BI : l'inscription Power BI n'a pas abouti. Tableau Public s'y substitue sans changer le modèle de données.
- Tableau Public ne se connecte qu'à des fichiers. Une commande (`./scripts/export_tableau.sh`) exporte les tables du DWH en CSV, et Tableau se rafraîchit dessus.
- En production, on brancherait Tableau Cloud ou Power BI directement sur le DWH. Le modèle ne change pas.

---

## Slide 9 — Démonstration en direct (4 min)

### Avant la soutenance

- Lancer `docker compose up -d` au moins 15 minutes avant.
- Vérifier que `SLACK_WEBHOOK_URL` est bien renseigné dans `.env`. Sans lui, le consommateur Slack tourne en mode simulation et n'envoie rien.
- Ouvrir quatre fenêtres :
  - un terminal à la racine du projet ;
  - Slack ;
  - Grafana (http://localhost:3000, dashboard « Pipeline Sport Data ») ;
  - Tableau, connecté à `exports/tableau/`.

### 1. Nouvelle course → Slack

« Je saisis une course de 10,8 km pour Jeannine Breton, directement dans la base source. »

```bash
docker exec -i postgres-source sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
insert into activites (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire)
values (81676, now() - interval '46 minutes', 'Runing', 10800, now(), 'Sortie au bord du Lez');
SQL
```

- Quelques secondes plus tard, Slack affiche : « Bravo Jeannine Breton ! Tu viens de courir 10,8 km en 46 min ! … »
- À dire : « Debezium a capté l'insertion, Kafka l'a diffusée, le consommateur Slack l'a contrôlée puis publiée. »

### 2. Taux de prime 5 % → 7 %

Ouvrir `dbt/seeds/parametres_avantages.csv` et remplacer `0.05` par `0.07`.

```bash
sleep 30   # le consommateur MinIO écrit ses lots toutes les 30 s
docker exec consumer-minio dbt build --no-use-colors | tail -1
./scripts/export_tableau.sh
```

- Le build dure moins d'une minute.
- La nouvelle course est maintenant dans le DWH, et toutes les primes sont recalculées avec le nouveau taux.

### 3. Visualiser

- Dans **Tableau** : *Données → Actualiser*.
  - La course apparaît dans les activités.
  - La colonne `taux_prime_sportive` vaut 0,07.
  - Le coût total augmente.
- Dans **Grafana** : le coût total fait un saut. Le taux appliqué est historisé à chaque build.
- À dire : « Un seul fichier modifié, aucun code. Tout l'historique est recalculé à partir du brut. »
- Remettre `0.05` après la démo.

### Plan B si le réseau ou Slack tombe

- Montrer le log du consommateur : `docker logs --tail 5 consumer-slack`.
- Montrer le panneau « Notifications Slack » de Grafana.

---

## Slide 10 — Prêt pour la production (1 min)

- **Pour le métier** :
  - le coût des avantages est connu et ajustable sans développeur ;
  - les déclarations de déplacement incohérentes sont détectées avant paiement ;
  - les salariés sont félicités en temps réel, ce qui favorise l'engagement.
- **Pour la DSI**, la migration AWS est directe :
  - Kafka → Amazon MSK ;
  - MinIO → S3 ;
  - PostgreSQL → RDS ;
  - le projet dbt reste identique.
- **Prochaines étapes** :
  - orchestrer dbt avec Airflow plutôt que depuis le consommateur ;
  - envoyer des alertes Grafana sur Slack ;
  - gérer les secrets dans AWS Secrets Manager.
- Conclure : « Je vous propose de valider le passage en production. Merci, je suis prêt pour vos questions. »

---

# Préparation de la discussion (10 min)

## Pertinence des outils

| Outil | Pourquoi ce choix | Alternative écartée |
|---|---|---|
| Debezium (CDC) | Capte les changements sans modifier l'application source | Requêtes périodiques : latence et suppressions invisibles |
| Kafka | Découple source et consommateurs, permet de rejouer l'historique | Appels directs : un consommateur en panne bloque tout |
| MinIO + Delta Lake | Format ouvert, transactions ACID, historique des versions, compatible S3 | Fichiers CSV bruts : pas de transactions ni de schéma |
| dbt | SQL versionné, tests, lignage, documentation | Scripts Python ad hoc : peu testables |
| DuckDB | Lit Delta, Excel et S3 nativement, sans cluster | Spark : surdimensionné pour ce volume |
| PostgreSQL DWH | Standard, connecteur natif dans tous les outils BI | Data warehouse cloud : coût injustifié pour un POC |
| Grafana | Open source, se branche directement sur PostgreSQL | Outil SaaS payant |
| SerpApi | Itinéraires Google Maps à pied et à vélo, résultats mis en cache | Calcul à vol d'oiseau : sous-estime la distance réelle |

## Choix face aux défis

- **Temps réel** : le CDC et Kafka publient le message Slack en quelques secondes.
- **Recalcul de l'historique** :
  - le brut est conservé en ajout seul ;
  - les marts sont reconstruites à chaque build ;
  - un paramètre modifié s'applique donc à tout l'historique.
  - En dernier recours, Kafka peut être relu depuis le début.
- **Coût des API** : la distance est calculée une fois par salarié, adresse et mode. Les adresses introuvables sont aussi mises en cache pour ne pas repayer.
- **Données sources modifiées** : Debezium capte aussi les mises à jour. Le staging garde la dernière version de chaque activité.

## Scalabilité

- **Kafka** : ajouter des partitions et des consommateurs dans le même groupe.
- **Stockage** : Delta sur S3 n'a pas de limite pratique de volume.
- **Transformations** : DuckDB suffit largement pour une ETI. Au-delà, le même projet dbt peut tourner sur Redshift, Snowflake ou Databricks en changeant d'adaptateur.
- **Point d'attention honnête** :
  - aujourd'hui, dbt est lancé par le consommateur MinIO ;
  - en production, un orchestrateur (Airflow) le découplerait ;
  - Kafka tourne sur un seul broker, il en faudrait au moins trois.

## Qualité des flux

- **Contrôles explicites** : chaque rejet porte un motif, compté dans Grafana.
- **Tests dbt** : ils sont bloquants à chaque build.
- **Doublons** : la livraison Kafka est « au moins une fois », et le dédoublonnage est fait en staging.
- **Idempotence** : l'offset Kafka n'est validé qu'après l'écriture dans MinIO, donc on ne perd rien en cas de crash.
- **Fraîcheur** : la date de dernière ingestion et la date de la dernière activité sont suivies dans Grafana.
- **Évolution possible** : des contrôles Soda ou Great Expectations sur le brut, avant dbt.

## Questions pièges probables

- **« Pourquoi pas Power BI ? »** L'inscription n'a pas abouti. Le DWH est standard, donc Power BI s'y brancherait sans modification.
- **« Tableau Public publie en ligne : et les salaires ? »**
  - Oui, c'est une limite du POC, sur des données fictives.
  - En production : Tableau Cloud ou Power BI avec accès restreint.
  - Les exports sont exclus de Git.
- **« Et si un salarié change d'adresse ? »** La distance est recalculée automatiquement, car le cache est indexé sur l'adresse.
- **« Et le RGPD ? »**
  - Données minimales (pas d'adresse dans les marts).
  - Secrets hors du code.
  - Accès au DWH à restreindre par rôle en production.
