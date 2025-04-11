{% macro update_job_param(success=true) %}

  {% if execute %}
    {% set upstream_nodes = graph.nodes[model.unique_id].depends_on.nodes %}
  {% else %}
    {% set upstream_nodes = [] %}
  {% endif %}

  
  {% if flags.FULL_REFRESH %}
    {% set delete_target_hwm_rows %}
        delete from {{ get_hwm_fqn() }}
        where target_name = '{{ model.unique_id }}';
    {% endset %}
    {% set delete_hwm_rows = run_query(delete_target_hwm_rows) %}
  {% endif %}
  
  {% for upstream_node in upstream_nodes %}
    {% set model_node = graph.nodes.get(upstream_node) %}
    {% set source_node = graph.sources.get(upstream_node) %}

    {% if model_node and model_node.resource_type == 'model' %}
      {% set source_name_db = model_node.database %}
      {% set source_name_schema = model_node.schema %}
      {% set source_name_alias = model_node.alias %}

    {% elif source_node and source_node.resource_type == 'source' %}
      {% set source_name_db = source_node.database %}
      {% set source_name_schema = source_node.schema %}
      {% set source_name_alias = source_node.name %}

    {% endif %}

    {% set job_param_sql %}
      insert into {{ get_hwm_fqn() }} (target_name, source_name, invocation_id, invocation_time, complete, hwm_timestamp)
      select
        target_name,
        source_name,
        invocation_id,
        invocation_time,
        {{ success }} as complete,
        hwm_timestamp
      from {{ get_hwm_tmp_fqn() }}
      where complete = false
        and source_name ilike '{{ source_name_db }}.{{ source_name_schema }}.{{ source_name_alias }}'
        and target_name = '{{ model.unique_id }}'
      order by hwm_timestamp desc
      limit 1;
    {% endset %}
    
    {% set update_hwm = run_query(job_param_sql) %}
  {% endfor %}
{% endmacro %}