with source as (
    select *
    from {{ source('jaffle_erp1','raw_products') }}
),
renamed as (
    select
        SKU as product_id,
        NAME as name,
        TYPE as type,
        PRICE as price,
        DESCRIPTION as description,
        cast(load_dts as timestamp) as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed