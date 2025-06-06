with source as (
    select *
    from {{ source('jaffle_erp1','raw_items') }}
),
renamed as (
    select
        ID as item_id,
        ORDER_ID as order_id,
        SKU as product_id,
        cast(LOAD_DTS as timestamp) as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
