with source as (
    select *
    from {{ source('jaffle_erp1','raw_stores') }}
),
renamed as (
    select
        ID as store_id,
        NAME as name,
        OPENED_AT as opened_on,
        TAX_RATE as tax_rate,
        cast(LOAD_DTS as timestamp) as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed