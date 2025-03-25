{% macro create_hwm_table() %}
  create table {{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node) }}.{{ var('watermark_table', 'dbt_high_watermark') }} if not exists (
    target_name text not null,
    source_name text not null,
    invocation_id text not null,
    complete boolean,
    source_timestamp timestamp_ntz(9) not null)

{% endmacro %}


{% macro create_tmp_hwm_table() %}
  {# dbt reuses snowflake sessions across models, one per thread #}
  {% set create_tmp_hwm_table %}
    create temporary table {{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node) }}.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }} (
    target_name text not null,
    source_name text not null,
    invocation_id text not null,
    complete boolean,
    source_timestamp timestamp_ntz(9) not null
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
  {% if execute %}
    {% set source_unique_id = get_node_unique_id_by_relation(relation_obj) %}
  {% else %}
    {% set source_unique_id = '' %}
  {% endif %}
  {% if current %}

    select max(source_timestamp) from {{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node) }}.hwm_tmp_{{ thread_id.split(' ')[0] | replace('-', '_') | lower }}
    where target_name = '{{ model.unique_id }}'
      and source_name = '{{ source_unique_id }}'

    
  {% else %}

    select max(source_timestamp) from {{ var('watermark_database', target.database) }}.{{ generate_schema_name(custom_schema_name=var('watermark_schema', 'public'), node) }}.{{ var('watermark_table', 'dbt_high_watermark') }}
    where target_name = '{{ model.unique_id }}'
      and source_name = '{{ source_unique_id }}'
      and complete = true


  {% endif %}
{% endmacro %}


{% macro get_node_unique_id_by_relation(relation_obj) %}
    {% set source_unique_id = none %}
    
    {% if execute %}
        {% set database = relation_obj.database %}
        {% set schema = relation_obj.schema %}
        {% set identifier = relation_obj.identifier %}
        {# First, loop through models in graph.nodes #}
        {% for node in graph.nodes %}
            {% if node.database == database and node.schema == schema and node.identifier == identifier %}
                {% set source_unique_id = node.unique_id %}
                {% break %}
            {% endif %}
        {% endfor %}

        {# If not found in models, search through sources in graph.sources #}
        {% if source_unique_id is none %}
            {% for source in graph.sources %}
                {% if source.database == database and source.schema == schema and source.identifier == identifier %}
                    {% set source_unique_id = source.unique_id %}
                    {% break %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    
    {# Return the unique ID if found, otherwise return a default or None #}
    {{ return(source_unique_id) }}
{% endmacro %}
