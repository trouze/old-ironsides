{% macro remove_mr_schemas() %}
{%- set get_mr_schemas_sql -%}
    show schemas like 'ci\\_%'
{%- endset -%}

{%- set results = run_query(get_mr_schemas_sql) -%}

{%- if execute -%}
    {%- for schema in results -%}

        {%- set drop_schema_sql -%}
            drop schema if exists {{ target.database ~ "." ~ schema['name'] }}
        {%- endset -%}

        {{ drop_schema_sql }};
        {% do run_query(drop_schema_sql) %}

    {%- endfor -%}
{%- endif -%}
{% endmacro %}