-- Post-hook des marts : copie de la table en Parquet dans le bucket clean de MinIO
{% macro export_minio_clean() -%}
    copy (select * from {{ this }})
    to 's3://{{ env_var("MINIO_BUCKET_CLEAN", "clean") }}/marts/{{ this.name }}.parquet'
    (format parquet)
{%- endmacro %}
