select
    cast("ID salarié" as integer) as id_salarie,
    trim("Nom") as nom,
    trim("Prénom") as prenom,
    cast("Date de naissance" as date) as date_naissance,
    "BU" as bu,
    cast("Date d'embauche" as date) as date_embauche,
    cast("Salaire brut" as integer) as salaire_brut,
    "Type de contrat" as type_contrat,
    cast("Nombre de jours de CP" as integer) as nb_jours_cp,
    "Adresse du domicile" as adresse_domicile,
    "Moyen de déplacement" as moyen_deplacement
from {{ source('excel', 'donnees_rh') }}
