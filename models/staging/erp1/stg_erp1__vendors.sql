with source as (
    select *
    from {{ source('jaffle_erp1','raw_vendors') }}
),
renamed as (
    select
        ID as vendor_id,
        NAME as name,
        SIGNUP_DATE as signup_date,
        LOAD_DTS as load_dts,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
