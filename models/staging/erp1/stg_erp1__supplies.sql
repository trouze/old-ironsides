with source as (
    select *
    from {{ source('jaffle_erp1','raw_supplies') }}
),
renamed as (
    select
        ID as supply_id,
        NAME as name,
        COST as cost,
        SKU as product_id,
        cast(LOAD_DTS as timestamp) as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed