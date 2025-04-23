{{ config(
    hwm_field='meta_last_touch_dtm',
    materialized='incremental',
    unique_key='vendor_id'
)}}

with dim_vendors_keys as (
  select
    vendor_id,
    meta_last_touch_dtm
  from {{ ref('dim_vendors') }}
  {% if is_incremental() %}
  where meta_last_touch_dtm between ({{ get_previous_hwm(ref('dim_vendors')) }}) and ({{ get_current_hwm(ref('dim_vendors')) }})
  {% endif %}
),

dim_vendors_pilot_keys as (
  select
    vendor_id,
    meta_last_touch_dtm
  from {{ ref('dim_vendors_pilot') }}
  {% if is_incremental() %}
  where meta_last_touch_dtm between ({{ get_previous_hwm(ref('dim_vendors_pilot')) }}) and ({{ get_current_hwm(ref('dim_vendors_pilot')) }})
  {% endif %}
),

fct_orders_keys as (
  select
    vendor_id,
    meta_last_touch_dtm
  from {{ ref('fct_orders') }}
  {% if is_incremental() %}
  where meta_last_touch_dtm between ({{ get_previous_hwm(ref('fct_orders')) }}) and ({{ get_current_hwm(ref('fct_orders')) }})
  {% endif %}
),

vendor_keys as (
  select vendor_id from dim_vendors_keys
  union
  select vendor_id from dim_vendors_pilot_keys
  union
  select vendor_id from fct_orders_keys
),

final as (
  select
    vendor_keys.vendor_id as vendor_id,
    coalesce(sum(fct_orders.order_total), 0) as lifetime_spend
  from vendor_keys
  left join {{ ref('fct_orders') }} fct_orders
    on vendor_keys.vendor_id = fct_orders.vendor_id
  group by 1
)

select * from final
