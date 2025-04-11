{% macro get_hwm_fqn(schema=false) %}
  {%- if schema -%}
{{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node=node) }}
  {%- else -%}
{{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node=node) }}.{{ var('watermark_table', 'dbt_high_watermark') }}
  {%- endif -%}
{% endmacro %}

{% macro get_hwm_tmp_fqn() %}
{{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node=node) }}.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }}
{% endmacro %}

{% macro create_hwm_table() %}

  {% set create_hwm_table %}
    create table {{ get_hwm_fqn() }} if not exists (
        target_name text not null,
        source_name text not null,
        invocation_id text not null,
        invocation_time timestamp_ntz(9) not null,
        complete boolean,
        hwm_timestamp timestamp_ntz(9) not null)
  {% endset %}

  {% do run_query(create_hwm_table) %}
{% endmacro %}


{% macro create_watermark_schema() %}

  {% set create_watermark_schema %}
    create schema if not exists {{ get_hwm_fqn(schema=true) }}
  {% endset %}

  {% do run_query(create_watermark_schema) %}
{% endmacro %}


{% macro create_tmp_hwm_table() %}
  {# dbt reuses snowflake sessions across models, one per thread #}
  {% set create_tmp_hwm_table %}
    create temporary table {{ get_hwm_tmp_fqn() }} (
    target_name text not null,
    source_name text not null,
    invocation_id text not null,
    invocation_time timestamp_ntz(9) not null,
    complete boolean,
    hwm_timestamp timestamp_ntz(9) not null
    )
  {% endset %}
  {% do run_query(create_tmp_hwm_table) %}
{% endmacro %}


{% macro get_current_hwm(relation_obj) %}
  {{ return(adapter.dispatch('get_hwm', project_name)(true, relation_obj)) }}
{% endmacro %}


{% macro get_previous_hwm(relation_obj) %}
  {{ return(adapter.dispatch('get_hwm', project_name)(false, relation_obj)) }}
{% endmacro %}


{% macro snowflake__get_hwm(current, relation_obj) %}

  {%- if current -%}
    select coalesce(
      max(hwm_timestamp),
      '1900-01-01 00:00:00.000'
    )
    from {{ get_hwm_tmp_fqn() }}
    where target_name = '{{ model.unique_id }}'
      and source_name ilike '{{ relation_obj }}'
    
  {%- else -%}

    select max(hwm_timestamp)
    from {{ get_hwm_fqn() }}
    where target_name = '{{ model.unique_id }}'
      and source_name ilike '{{ relation_obj }}'
      and complete = true

  {%- endif -%}
{% endmacro %}