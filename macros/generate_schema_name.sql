{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if env_var('DBT_CLOUD_ENVIRONMENT_TYPE','empty') == 'empty' or env_var('DBT_CLOUD_ENVIRONMENT_TYPE') == 'staging' or env_var('DBT_CLOUD_ENVIRONMENT_TYPE') == 'prod' or var('schema_behavior') == 'deploy' -%}
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}
    
    {%- elif env_var('DBT_CLOUD_ENVIRONMENT_TYPE') == 'dev' or target.name == 'dev' or target.name == 'ci' -%}
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}

        {%- endif -%}
    {% else %}
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}
    {% endif %}
{%- endmacro %}
