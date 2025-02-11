select
    customer_id,
    name,
    signup_date,
    current_timestamp() as last_model_run,
    'test' as test
from {{ ref('stg_customers') }}