select
    vendor_id,
    name,
    signup_date,
    current_timestamp() as last_model_run
from {{ ref('stg_erp2__vendors') }}