{% snapshot dim_sales %}

{{
    config(
        target_schema = 'public',
        unique_key = 'order_id',
        strategy = 'check',
        check_cols = [
            'order_date',
            'store_code',
            'customer_id',
            'product_id',
            'quantity',
            'unit_price',
            'total_amount'
        ]
    )
}}

SELECT
    order_id,
    order_date,
    store_code,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    partition_time
FROM {{ source('public', 'src_sales') }}

{% endsnapshot %}