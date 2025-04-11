{{ config(
    hwm_field='load_dts',
    materialized='incremental',
    unique_key='customer_id',
    pre_hook=["{{ set_job_param() }}"],
    post_hook=["{{ update_job_param(success=true) }}"]
)}}

with raw_orders as (
  
  select * from {{ source('jaffle_shop', 'raw_orders') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(source('jaffle_shop', 'raw_orders')) }}) and ({{ get_current_hwm(source('jaffle_shop', 'raw_orders')) }})
  {% endif %}

),

stg_customers as (
  select * from {{ ref('stg_customers') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(ref('stg_customers')) }}) and ({{ get_current_hwm(ref('stg_customers')) }})
  {% endif %}
),

final as (
    select
      o.ID,
      o.customer as customer_id,
      c.name,
      o.ordered_at,
      o.load_dts
    from raw_orders o
    left join stg_customers c
      on o.customer = c.customer_id
)
select * from final