{{ config(
    materialized='incremental',
    unique_key=['initial_store_code','store_code','start_date'],
    incremental_strategy='merge'
) }}

WITH src AS (

    SELECT
        CAST(store_code AS TEXT) AS store_code,
        CAST(latitude AS TEXT) AS latitude,
        CAST(longitude AS TEXT) AS longitude,
        partition_time::date AS start_date
    FROM {{ source('public', 'src_store') }}
    WHERE partition_time::date = DATE '{{ var("raw_data_date") }}'

),

initial_store AS (

    SELECT
        store_code AS initial_store_code,
        latitude,
        longitude
    FROM (
        SELECT
            store_code,
            latitude,
            longitude,
            ROW_NUMBER() OVER(
                PARTITION BY latitude, longitude
                ORDER BY partition_time
            ) AS rn
        FROM {{ source('public', 'src_store') }}
    ) s
    WHERE rn = 1

),

store_mapping AS (

    SELECT
        i.initial_store_code,
        s.store_code,
        s.start_date
    FROM src s
    JOIN initial_store i
        ON s.latitude = i.latitude
        AND s.longitude = i.longitude

)

{% if is_incremental() %}

, closed_rows AS (

    SELECT
        t.initial_store_code,
        t.store_code,
        t.start_date,
        m.start_date AS end_date
    FROM {{ this }} t
    JOIN store_mapping m
        ON t.initial_store_code = m.initial_store_code
       AND t.end_date = DATE '9999-12-31'
       AND m.store_code <> t.store_code

)

, new_rows AS (

    SELECT
        m.initial_store_code,
        m.store_code,
        m.start_date,
        DATE '9999-12-31' AS end_date
    FROM store_mapping m
    LEFT JOIN {{ this }} t
        ON m.initial_store_code = t.initial_store_code
       AND m.store_code = t.store_code
       AND t.end_date = DATE '9999-12-31'
    WHERE t.initial_store_code IS NULL

)

SELECT * FROM closed_rows
UNION ALL
SELECT * FROM new_rows

{% else %}

SELECT
    initial_store_code,
    store_code,
    start_date,
    DATE '9999-12-31' AS end_date
FROM store_mapping

{% endif %}