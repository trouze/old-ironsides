{% macro log_env() %}

    {{ log(env_var('DBT_CLOUD_ENVIRONMENT_ID'), info=true) }}

{% endmacro %}