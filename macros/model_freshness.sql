{% test is_model_fresh(model, last_model_run_field, error_after, time_part) %}
{{ config(store_failures = false, severity = 'error') }}
    select
    count(*)
    from {{ model }}
    having cast(max({{ last_model_run_field }}) as TIMESTAMP) < DATEADD({{ time_part }}, -cast({{ error_after }} as integer), current_timestamp())
{% endtest %}