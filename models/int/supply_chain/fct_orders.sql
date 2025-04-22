select
    order_id,
    customer_id,
    order_timestamp,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    current_timestamp() as last_model_run
from {{ ref('stg_erp1__orders') }}