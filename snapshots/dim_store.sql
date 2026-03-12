{% snapshot dim_store %}
{{
    config(
        target_schema='public',
        unique_key='store_code',
        strategy='check',
        check_cols=[
            'store_description',
            'country',
            'area',
            'city',
            'district',
            'zip',
            'channel_type',
            'gross_area',
            'net_sales_area',
            'area_unit',
            'latitude',
            'longitude',
            'opening_date',
            'closing_date',
            'local_currency',
            'format'
        ]
    )
}}

SELECT
    store_code,
    store_description,
    country,
    area,
    city,
    district,
    zip,
    channel_type,
    gross_area,
    net_sales_area,
    area_unit,
    latitude,
    longitude,
    opening_date,
    closing_date,
    local_currency,
    format,
    partition_time AS partition_date
FROM {{ source('public', 'src_store_detail') }}
WHERE partition_time = '{{ var("raw_data_date") }}'::date

{% endsnapshot %}