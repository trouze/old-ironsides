select
    vendor_id,
    name,
    signup_date,
    meta_last_touch_dtm
from {{ ref('stg_erp1__vendors') }}