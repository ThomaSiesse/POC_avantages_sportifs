

    insert into "dwh"."monitoring"."mon_etat_pipeline" ("execute_le", "nb_lignes_raw", "derniere_ingestion_minio", "nb_activites", "nb_activites_valides", "nb_activites_rejetees", "derniere_activite", "nb_eligibles_prime", "nb_eligibles_bien_etre", "cout_total_avantages", "nb_deplacements_incoherents", "nb_deplacements_non_calcules")
    (
        select "execute_le", "nb_lignes_raw", "derniere_ingestion_minio", "nb_activites", "nb_activites_valides", "nb_activites_rejetees", "derniere_activite", "nb_eligibles_prime", "nb_eligibles_bien_etre", "cout_total_avantages", "nb_deplacements_incoherents", "nb_deplacements_non_calcules"
        from "mon_etat_pipeline__dbt_tmp20261029134545463006"
    )
  