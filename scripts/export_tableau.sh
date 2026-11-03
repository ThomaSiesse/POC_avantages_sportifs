#!/usr/bin/env bash
# Exporte les tables du schéma marts de postgres-dwh en CSV pour Tableau Public,
# qui ne se connecte qu'à des fichiers locaux.
#
# Usage (depuis la racine du projet, postgres-dwh démarré) :
#   ./scripts/export_tableau.sh [dossier_de_sortie]     (défaut : exports/tableau)
set -euo pipefail

cd "$(dirname "$0")/.."
OUT_DIR="${1:-exports/tableau}"
mkdir -p "$OUT_DIR"

env_val() { grep "^$1=" .env | cut -d= -f2- | xargs; }
DWH_USER="$(env_val DWH_USER)"
DWH_DB="$(env_val DWH_DB)"
psql_dwh() { docker exec -i postgres-dwh psql -U "$DWH_USER" -d "$DWH_DB" -At "$@"; }

tables=$(psql_dwh -c "select table_name from information_schema.tables
                      where table_schema = 'marts' order by table_name")

for table in $tables; do
    # Formats lisibles par Tableau : booléens en true/false (Postgres écrit t/f),
    # dates-heures en heure de Paris sans fuseau ni microsecondes
    colonnes=$(psql_dwh -c "select string_agg(
            case data_type
                when 'boolean' then format('%I::text as %I', column_name, column_name)
                when 'timestamp with time zone' then format(
                    'to_char(%I at time zone ''Europe/Paris'', ''YYYY-MM-DD HH24:MI:SS'') as %I',
                    column_name, column_name)
                else format('%I', column_name)
            end, ', ' order by ordinal_position)
        from information_schema.columns
        where table_schema = 'marts' and table_name = '$table'")
    psql_dwh -c "\copy (select $colonnes from marts.$table) to stdout with (format csv, header)" \
        > "$OUT_DIR/$table.csv"
    echo "$OUT_DIR/$table.csv : $(($(wc -l < "$OUT_DIR/$table.csv") - 1)) lignes"
done
