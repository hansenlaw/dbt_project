{{ config(
    materialized    = 'incremental',
    unique_key      = 'order_id'
) }}

SELECT
    s.order_id,
    s.order_date,
    s.store_code,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.total_amount,
    s.partition_time
FROM {{ source('public', 'src_sales') }} s
WHERE s.partition_time = '{{ var("raw_data_date") }}'::date

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.order_id = s.order_id
)
{% endif %}
