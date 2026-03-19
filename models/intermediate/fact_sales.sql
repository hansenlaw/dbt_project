{{ config(
    materialized    = 'incremental',
    unique_key      = 'order_id',
    post_hook       = [
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_order_date  ON {{ this }} (order_date)",
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_customer_id ON {{ this }} (customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_store_code  ON {{ this }} (store_code)",
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_product_id  ON {{ this }} (product_id)"
    ]
) }}

SELECT
    s.order_id,
    s.order_date,
    {{ clean_id('s.store_code') }}    AS store_code,
    {{ clean_id('s.customer_id') }}   AS customer_id,
    {{ clean_id('s.product_id') }}    AS product_id,
    s.quantity,
    s.unit_price,
    s.total_amount,
    s.partition_time
FROM {{ source('public', 'src_sales') }} s
WHERE s.partition_time <= '{{ var("raw_data_date") }}'::date

{% if is_incremental() %}
AND NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.order_id = s.order_id
)
{% endif %}
