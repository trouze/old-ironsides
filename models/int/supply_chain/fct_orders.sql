select
    order_id,
    vendor_id,
    order_timestamp,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    meta_last_touch_dtm
from {{ ref('stg_erp1__orders') }}