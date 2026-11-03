

    insert into "warehouse"."intermediate"."int_distances_domicile" ("id_salarie", "adresse_domicile", "moyen_deplacement", "distance_km", "calcule_le")
    (
        select "id_salarie", "adresse_domicile", "moyen_deplacement", "distance_km", "calcule_le"
        from "int_distances_domicile__dbt_tmp20261029134539697599"
    )
  