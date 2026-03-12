{{ config(materialized='table') }}

/*
  Untuk: Store Manager
  Isi  : Direktori lengkap toko — kode awal, kode aktif saat ini,
         status (active/closed), riwayat pergantian kode, dan total revenue
  Kegunaan:
    - Lihat toko mana yang masih aktif dan sudah tutup
    - Lacak riwayat pergantian kode toko (SCD2)
    - Lihat kontribusi revenue lifetime tiap toko
*/

WITH store_history AS (

    SELECT
        initial_store_code,
        store_code,
        start_date,
        end_date,
        CASE
            WHEN end_date = DATE '9999-12-31' THEN 'active'
            ELSE 'closed'
        END AS status,
        ROW_NUMBER() OVER (
            PARTITION BY initial_store_code
            ORDER BY start_date
        ) AS version_number
    FROM {{ ref('working_initial_store') }}

),

current_code AS (

    SELECT initial_store_code, store_code AS current_store_code, start_date AS active_since
    FROM store_history
    WHERE status = 'active'

),

code_change_count AS (

    SELECT
        initial_store_code,
        COUNT(*) - 1 AS total_code_changes   -- jumlah kali kode toko berubah
    FROM store_history
    GROUP BY 1

),

lifetime_revenue AS (

    SELECT
        wis.initial_store_code,
        SUM(fs.total_amount) AS lifetime_revenue,
        COUNT(DISTINCT fs.order_id) AS lifetime_orders
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('working_initial_store') }} wis
        ON  fs.store_code = wis.store_code
        AND fs.order_date >= wis.start_date
        AND fs.order_date <  wis.end_date
    GROUP BY 1

)

SELECT
    h.initial_store_code,
    c.current_store_code,
    h.status,
    c.active_since,
    cc.total_code_changes,
    COALESCE(lr.lifetime_revenue, 0) AS lifetime_revenue,
    COALESCE(lr.lifetime_orders, 0)  AS lifetime_orders
FROM (
    -- ambil satu baris per toko (representasi current/last status)
    SELECT DISTINCT ON (initial_store_code)
        initial_store_code, status
    FROM store_history
    ORDER BY initial_store_code, start_date DESC
) h
LEFT JOIN current_code       c   ON h.initial_store_code = c.initial_store_code
LEFT JOIN code_change_count  cc  ON h.initial_store_code = cc.initial_store_code
LEFT JOIN lifetime_revenue   lr  ON h.initial_store_code = lr.initial_store_code
ORDER BY h.initial_store_code
