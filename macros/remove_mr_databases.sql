{% macro remove_mr_databases() %}
{%- set get_mr_databases_sql -%}
    show databases like 'ci\\_%'
{%- endset -%}

{%- set results = run_query(get_mr_databases_sql) -%}

{%- if execute -%}
    {%- for database in results -%}

        {%- set drop_database_sql -%}
            drop database if exists {{ database['name'] }}
        {%- endset -%}
        
        {{ drop_database_sql }};
        {% do run_query(drop_database_sql) %}

    {%- endfor -%}
{%- endif -%}
{% endmacro %}