with source as (
    select * from {{source('jaffle_erp1','raw_orders')}}
),
renamed as (
    select
        ID as order_id,
        CUSTOMER as vendor_id,
        cast(ORDERED_AT as timestamp) as order_timestamp,
        STORE_ID as store_id,
        SUBTOTAL as subtotal,
        TAX_PAID as tax_paid,
        ORDER_TOTAL as order_total,
        cast(load_dts as timestamp) as meta_last_touch_dtm
    from source
)
select * from renamed
