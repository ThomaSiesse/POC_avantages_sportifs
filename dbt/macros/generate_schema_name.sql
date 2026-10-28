-- Utilise le schéma configuré tel quel (marts, staging...) au lieu de main_marts
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
