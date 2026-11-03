-- Activités retenues pour les marts (les rejets restent visibles dans int_activites_controlees)
select * exclude (motif_rejet, salarie_connu, vitesse_max_kmh)
from "warehouse"."intermediate"."int_activites_controlees"
where motif_rejet is null