{{ config(
    hwm_field='load_dts',
    materialized='incremental',
    unique_key='customer_id'
)}}

with dim_vendors as (
  
  select * from {{ ref('dim_vendors') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(ref('dim_vendors')) }}) and ({{ get_current_hwm(ref('dim_vendors')) }})
  {% endif %}

),

dim_vendors_pilot as (

  select * from {{ ref('dim_vendors_pilot') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(ref('dim_vendors_pilot')) }}) and ({{ get_current_hwm(ref('dim_vendors_pilot')) }})
  {% endif %}

)

fct_orders as (
  select * from {{ ref('fct_orders') }}
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