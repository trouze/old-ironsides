{{ config(
    hwm_field='meta_last_touch_dtm',
    materialized='incremental',
    unique_key='vendor_id'
)}}

with dim_vendors as (
  
  select
    vendor_id,
    meta_last_touch_dtm
  from {{ ref('dim_vendors') }}
  {% if is_incremental() %}
  where meta_last_touch_dtm between ({{ get_previous_hwm(ref('dim_vendors')) }}) and ({{ get_current_hwm(ref('dim_vendors')) }})
  {% endif %}

),

dim_vendors_pilot as (

  select
    vendor_id,
    meta_last_touch_dtm
  from {{ ref('dim_vendors_pilot') }}
  {% if is_incremental() %}
  where meta_last_touch_dtm between ({{ get_previous_hwm(ref('dim_vendors_pilot')) }}) and ({{ get_current_hwm(ref('dim_vendors_pilot')) }})
  {% endif %}

),

fct_orders as (
  select * from {{ ref('fct_orders') }}
),

vendor_keys as (
  select vendor_id from dim_vendors
  union
  select vendor_id from dim_vendors_pilot
),

final as (
  select
    vendor_keys.vendor_id as vendor_id,
    sum(fct_orders.order_total) as lifetime_spend
  from vendor_keys
  left join fct_orders
    on vendor_keys.vendor_id = fct_orders.vendor_id
  group by 1
)

select * from final
