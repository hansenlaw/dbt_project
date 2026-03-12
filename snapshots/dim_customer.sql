{% snapshot dim_customer %}

{{
    config(
        target_schema = 'public',
        unique_key    = 'customer_id',
        strategy      = 'check',
        check_cols    = [
            'full_name',
            'email',
            'phone',
            'city',
            'province',
            'tier'
        ]
    )
}}

SELECT
    customer_id,
    full_name,
    email,
    phone,
    city,
    province,
    registration_date,
    tier,
    partition_time AS partition_date
FROM {{ source('public', 'src_customer') }}
WHERE partition_time = '{{ var("raw_data_date") }}'::date

{% endsnapshot %}
