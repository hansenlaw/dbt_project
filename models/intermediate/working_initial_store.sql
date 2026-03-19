{{ config(
    materialized        = 'incremental',
    unique_key          = ['initial_store_code', 'store_code', 'start_date'],
    incremental_strategy = 'merge'
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- INCREMENTAL PATH: process new batch only, update SCD2 rows
-- ─────────────────────────────────────────────────────────────────────────────
{% if is_incremental() %}

WITH src AS (

    SELECT
        {{ clean_id('CAST(store_code AS TEXT)') }} AS store_code,
        CAST(latitude  AS TEXT) AS latitude,
        CAST(longitude AS TEXT) AS longitude,
        partition_time::date    AS start_date
    FROM {{ source('public', 'src_store') }}
    WHERE partition_time::date = DATE '{{ var("raw_data_date") }}'

),

initial_store AS (

    SELECT store_code AS initial_store_code, latitude, longitude
    FROM (
        SELECT
            {{ clean_id('CAST(store_code AS TEXT)') }} AS store_code,
            CAST(latitude  AS TEXT) AS latitude,
            CAST(longitude AS TEXT) AS longitude,
            ROW_NUMBER() OVER (
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
        ON  s.latitude  = i.latitude
        AND s.longitude = i.longitude

),

closed_rows AS (

    -- Close rows where the store code has changed this batch
    SELECT
        t.initial_store_code,
        t.store_code,
        t.start_date,
        m.start_date AS end_date
    FROM {{ this }} t
    JOIN store_mapping m
        ON  t.initial_store_code = m.initial_store_code
        AND t.end_date           = DATE '9999-12-31'
        AND m.store_code        <> t.store_code

),

new_rows AS (

    -- Add new rows for codes not yet in the table
    SELECT
        m.initial_store_code,
        m.store_code,
        m.start_date,
        DATE '9999-12-31' AS end_date
    FROM store_mapping m
    LEFT JOIN {{ this }} t
        ON  m.initial_store_code = t.initial_store_code
        AND m.store_code         = t.store_code
        AND t.end_date           = DATE '9999-12-31'
    WHERE t.initial_store_code IS NULL

)

SELECT * FROM closed_rows
UNION ALL
SELECT * FROM new_rows

-- ─────────────────────────────────────────────────────────────────────────────
-- FULL REFRESH PATH: build complete SCD2 history from all batches
-- ─────────────────────────────────────────────────────────────────────────────
{% else %}

WITH src_all AS (

    -- All distinct (store_code, lat, lon, batch_date) up to raw_data_date
    SELECT DISTINCT
        {{ clean_id('CAST(store_code AS TEXT)') }} AS store_code,
        CAST(latitude  AS TEXT) AS latitude,
        CAST(longitude AS TEXT) AS longitude,
        partition_time::date    AS batch_date
    FROM {{ source('public', 'src_store') }}
    WHERE partition_time::date <= DATE '{{ var("raw_data_date") }}'

),

initial_store AS (

    -- Stable identity: the very first store code ever seen at each location
    SELECT store_code AS initial_store_code, latitude, longitude
    FROM (
        SELECT
            {{ clean_id('CAST(store_code AS TEXT)') }} AS store_code,
            CAST(latitude  AS TEXT) AS latitude,
            CAST(longitude AS TEXT) AS longitude,
            ROW_NUMBER() OVER (
                PARTITION BY latitude, longitude
                ORDER BY partition_time
            ) AS rn
        FROM {{ source('public', 'src_store') }}
    ) s
    WHERE rn = 1

),

code_first_seen AS (

    -- First date each code appeared at a given location
    SELECT
        i.initial_store_code,
        s.store_code,
        MIN(s.batch_date) AS start_date
    FROM src_all s
    JOIN initial_store i
        ON  s.latitude  = i.latitude
        AND s.longitude = i.longitude
    GROUP BY 1, 2

),

scd2 AS (

    -- Assign end_date = start_date of the NEXT code for this store, else 9999-12-31
    SELECT
        initial_store_code,
        store_code,
        start_date,
        COALESCE(
            LEAD(start_date) OVER (PARTITION BY initial_store_code ORDER BY start_date),
            DATE '9999-12-31'
        ) AS end_date
    FROM code_first_seen

)

SELECT * FROM scd2

{% endif %}
