-- Une ligne par salarié : éligibilité et coût des deux avantages
--   Prime sportive   : déplacement domicile-travail sportif déclaré, non incohérent
--   Jours bien-être  : au moins N activités sur les 12 derniers mois
-- Paramètres dans les vars de dbt_project.yml
with salaries as (
    select * from {{ ref('dim_salaries') }}
),

activites as (
    select
        id_salarie,
        count(*) as nb_activites,
        count(distinct date_activite) as nb_jours_sport
    from {{ ref('fct_activites') }}
    group by id_salarie
),

eligibilite as (
    select
        salaries.id_salarie,
        salaries.salaire_brut,
        salaries.moyen_deplacement,
        salaries.statut_controle_deplacement,
        coalesce(activites.nb_activites, 0) as nb_activites_12_mois,
        coalesce(activites.nb_jours_sport, 0) as nb_jours_sport_12_mois,
        salaries.deplacement_sportif
            and salaries.statut_controle_deplacement <> 'incoherent' as eligible_prime,
        coalesce(activites.nb_activites, 0)
            >= {{ var('nb_activites_min_bien_etre') }} as eligible_bien_etre
    from salaries
    left join activites using (id_salarie)
)

select
    *,
    -- distance non vérifiable (adresse introuvable) : éligible sur déclaration, à contrôler
    eligible_prime and statut_controle_deplacement = 'non_calcule' as prime_a_verifier,
    case when eligible_prime
        then cast(salaire_brut * {{ var('taux_prime_sportive') }} as decimal(12, 2)) else 0
    end as montant_prime,
    case when eligible_bien_etre then {{ var('nb_jours_bien_etre') }} else 0 end as nb_jours_bien_etre,
    case when eligible_bien_etre
        then cast(salaire_brut / {{ var('nb_jours_travailles_an') }} * {{ var('nb_jours_bien_etre') }} as decimal(12, 2))
        else 0
    end as cout_jours_bien_etre
from eligibilite
