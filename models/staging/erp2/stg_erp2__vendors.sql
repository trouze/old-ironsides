with source as (
    select *
    from {{ source('jaffle_erp2','raw_vendors') }}
),
renamed as (
    select
        ID as vendor_id,
        NAME as name,
        cast(SIGNUP_DATE as timestamp) as signup_date,
        cast(LOAD_DTS as timestamp) as meta_last_touch_dtm,
        current_timestamp() as last_model_run
    from source
)
select * from renamed
