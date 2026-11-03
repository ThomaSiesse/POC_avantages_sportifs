

    insert into "dwh"."monitoring"."mon_rejets_activites" ("execute_le", "motif_rejet", "nb_activites")
    (
        select "execute_le", "motif_rejet", "nb_activites"
        from "mon_rejets_activites__dbt_tmp20261029134542980501"
    )
  