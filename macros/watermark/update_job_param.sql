{% macro update_job_param(success) %}

  {% if execute %}
    {% set upstream_nodes = graph.nodes[model.unique_id].depends_on.nodes %}
  {% else %}
    {% set upstream_nodes = [] %}
  {% endif %}
  
  
  {% if flags.FULL_REFRESH %}
    delete from {{ target.database }}.{{ var('watermark_schema', 'public') }}.{{ var('watermark_table', 'dbt_high_watermark') }}
    where target_name = '{{ model.unique_id }}';
  {% endif %}
  
  {% for upstream_node in upstream_nodes %}
    {% set model_node = graph.nodes.get(upstream_node) %}
    {% set source_node = graph.sources.get(upstream_node) %}

    {% if model_node and model_node.resource_type == 'model' %}
      {% set source_name = model_node.unique_id %}

    {% elif source_node and source_node.resource_type == 'source' %}
      {% set source_name = source_node.unique_id %}

    {% endif %}

    {% set job_param_sql %}
      insert into {{ target.database }}.{{ var('watermark_schema', 'public') }}.{{ var('watermark_table', 'dbt_high_watermark') }} (target_name, source_name, invocation_id, complete, source_timestamp)
      select
        target_name,
        source_name,
        invocation_id,
        {{ success }} as complete,
        source_timestamp
      from {{ target.database }}.public.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }}
      where success = false
        and source_name = '{{ source_name }}'
        and target_name = '{{ model.unique_id }}'
      order by source_timestamp desc
      limit 1;
    {% endset %}
    
    {% set update_hwm = run_query(job_param_sql) %}
  {% endfor %}
{% endmacro %}