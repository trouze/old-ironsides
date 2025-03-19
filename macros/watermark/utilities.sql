{% macro create_hwm_table() %}

  create table {{ target.database }}.{{ var('watermark_schema', 'public') }}.{{ var('watermark_table', 'dbt_high_watermark') }} if not exists (
    target_name text not null,
    source_name text not null,
    invocation_id binary(16) not null,
    complete boolean,
    source_timestamp timestamp_ntz(9) not null)

{% endmacro %}


{% macro create_tmp_hwm_table() %}
  {# dbt reuses snowflake sessions across models, one per thread #}
  {% set create_tmp_hwm_table %}
    create table {{ target.database }}.{{ var('watermark_schema', 'public') }}.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }} (
    target_name text not null,
    source_name text not null,
    invocation_id binary(16) not null,
    complete boolean,
    source_timestamp timestamp_ntz(9) not null
    )
  {% endset %}
  {% do run_query(create_tmp_hwm_table) %}
{% endmacro %}


{% macro get_previous_hwm(arg1, arg2=none) %}
  {% if execute %}
    select max(source_timestamp) from {{ target.database }}.public.dbt_high_watermark
    where target_name = '{{ model.unique_id }}'
      and source_name = '{{ get_node_unique_id(arg1, arg2) }}'
      and complete = true
  {% elif not execute %}
    '2020-01-01'
  {% endif %}
{% endmacro %}


{% macro get_current_hwm(arg1, arg2=none) %}
  {{ return(adapter.dispatch('get_hwm', project_name)(true, arg1, arg2)) }}
{% endmacro %}


{% macro get_previous_hwm(arg1, arg2=none) %}
  {{ return(adapter.dispatch('get_hwm', project_name)(false, arg1, arg2)) }}
{% endmacro %}


{% macro get_hwm(current, arg1, arg2=none) %}
  {% if execute %}
    {% set source_unique_id = get_node_unique_id() %}
  {% else %}
    {% set source_unique_id = '' %}
  {% endif %}
  {% if current %}

    select max(source_timestamp) from {{ target.database }}.public.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }}
    where target_name = '{{ model.unique_id }}'
      and source_name = '{{ source_unique_id }}'

    
  {% else %}

    select max(source_timestamp) from {{ target.database }}.{{ var('watermark_schema', 'public') }}.{{ var('watermark_table', 'dbt_high_watermark') }}
    where target_name = '{{ model.unique_id }}'
      and source_name = '{{ source_unique_id }}'
      and complete = true


  {% endif %}
{% endmacro %}


{% macro get_node_unique_id(arg1, arg2) %}
    {% if arg2 is none %}
        {# Single argument passed, assume it's a model #}
        {% set model_name = arg1 %}

        {{ return() }}
    {% else %}
        {# Two arguments passed, assume it's a source #}
        {% set source_name = arg1 %}
        {% set table_name = arg2 %}
        {{ return() }}
    {% endif %}
{% endmacro %}