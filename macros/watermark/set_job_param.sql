{% macro set_job_param(success=false) %}

  {# create temporary high watermark table(s) #}
  {{ create_watermark_schema() }}

  {# create high watermark table if it doesn't exist #}
  {%- set hwm_relation = adapter.get_relation(
      database=var('watermark_database', target.database),
      schema=generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node=node),
      identifier=var('watermark_table', 'dbt_high_watermark')) is not none -%}
  {% if not hwm_relation %}
    {{ create_hwm_table() }}
  {% endif %}

  {{ create_tmp_hwm_table() }}

  {% if execute %}
    {% set upstream_nodes = graph.nodes[model.unique_id].depends_on.nodes %}
  {% else %}
    {% set upstream_nodes = [] %}
  {% endif %}

  {% for upstream_node in upstream_nodes %}

    {% set model_node = graph.nodes.get(upstream_node) %}
    {% set source_node = graph.sources.get(upstream_node) %}

    {% if model_node and model_node.resource_type == 'model' %}

      {% set hwm_field = model.config.get('hwm_field', 'updated_at') %}
      {% set upstream_node_db = model_node.database %}
      {% set upstream_node_schema = model_node.schema %}
      {% set upstream_node_alias = model_node.alias %}

    {% elif source_node and source_node.resource_type == 'source' %}

      {% set loaded_at_field = source_node.loaded_at_field %}

      {% if model.config.get('use_loaded_at', false) and loaded_at_field %}
        {% set hwm_field = loaded_at_field %}
      {% else %}
        {% set hwm_field = model.config.get('hwm_field', 'updated_at')%}
      {% endif %}

      {% set upstream_node_db = source_node.database %}
      {% set upstream_node_schema = source_node.schema %}
      {% set upstream_node_alias = source_node.identifier %}
    {% endif %}

    {% set job_param_sql %}
      insert into {{ get_hwm_tmp_fqn() }} (
        target_name,
        source_name,
        invocation_id,
        invocation_time,
        complete,
        hwm_timestamp
      )
      select
        '{{ model.unique_id }}' as target_name,
        '{{ upstream_node_db }}.{{ upstream_node_schema }}.{{ upstream_node_alias }}' as source_name,
        '{{ invocation_id }}' as invocation_id,
        current_timestamp as invocation_time,
        {{ success }} as complete,
        max({{ hwm_field }}) as hwm_timestamp
      from {{ upstream_node_db }}.{{ upstream_node_schema }}.{{ upstream_node_alias }}
    {% endset %}
    {% do run_query(job_param_sql) %}
  {% endfor %}

{% endmacro %}