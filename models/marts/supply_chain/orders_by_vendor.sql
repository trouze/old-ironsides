with c as (
    select * from {{ ref('dim_vendors') }}
),
o as (
    select * from {{ ref('fct_orders') }}
),
final as (
    select
        c.vendor_id as vendor_id,
        c.name as name,
        min(o.order_timestamp) as first_order_date,
        max(o.order_timestamp) as last_order_date,
        count(o.order_timestamp) as num_orders,
        current_timestamp() as last_model_run
    from c
    left join o on o.vendor_id = c.vendor_id
    where o.vendor_id is not null
    group by c.vendor_id, c.name
)
select * from final
