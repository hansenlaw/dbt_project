{{ config(materialized='table') }}

/*
  Untuk: Tim Marketing / CRM
  Isi  : Direktori lengkap customer — profil terkini, tier saat ini,
         riwayat perubahan tier (SCD2), dan statistik transaksi lifetime
  Kegunaan:
    - Lihat profil lengkap setiap customer beserta tier aktif
    - Lacak riwayat perubahan tier customer (Bronze→Silver→Gold)
    - Lihat kontribusi lifetime value tiap customer
    - Dasar targeting kampanye berdasarkan tier & lokasi

  Catatan: model ini membutuhkan dbt snapshot dim_customer sudah dijalankan
           sebelum dbt run (`dbt snapshot --var 'raw_data_date: ...'`)
*/

WITH customer_history AS (

    -- seluruh riwayat SCD2 dari snapshot dim_customer
    SELECT
        customer_id,
        tier,
        dbt_valid_from,
        dbt_valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY dbt_valid_from
        ) AS version_number
    FROM {{ ref('dim_customer') }}

),

current_profile AS (

    -- ambil baris paling baru (dbt_valid_to IS NULL = masih aktif)
    SELECT
        customer_id,
        full_name,
        email,
        phone,
        city,
        province,
        registration_date,
        tier       AS current_tier,
        dbt_valid_from AS tier_since
    FROM {{ ref('dim_customer') }}
    WHERE dbt_valid_to IS NULL

),

tier_change_count AS (

    SELECT
        customer_id,
        COUNT(*) - 1 AS total_tier_upgrades   -- versi pertama bukan "perubahan"
    FROM customer_history
    GROUP BY 1

),

tier_journey AS (

    -- rekap perjalanan tier: Bronze→Silver atau Silver→Gold
    SELECT
        customer_id,
        STRING_AGG(tier, ' → ' ORDER BY dbt_valid_from) AS tier_journey
    FROM customer_history
    GROUP BY 1

),

lifetime_stats AS (

    SELECT
        customer_id,
        COUNT(DISTINCT order_id)     AS lifetime_orders,
        SUM(total_amount)            AS lifetime_spend,
        MIN(order_date)              AS first_purchase_date,
        MAX(order_date)              AS last_purchase_date,
        CURRENT_DATE - MAX(order_date) AS days_since_last_purchase
    FROM {{ ref('fact_sales') }}
    GROUP BY 1

)

SELECT
    cp.customer_id,
    cp.full_name,
    cp.email,
    cp.phone,
    cp.city,
    cp.province,
    cp.registration_date,
    CURRENT_DATE - cp.registration_date               AS days_as_customer,
    cp.current_tier,
    cp.tier_since,
    COALESCE(tc.total_tier_upgrades, 0)               AS total_tier_upgrades,
    tj.tier_journey,
    COALESCE(ls.lifetime_orders, 0)                   AS lifetime_orders,
    COALESCE(ls.lifetime_spend, 0)                    AS lifetime_spend,
    ls.first_purchase_date,
    ls.last_purchase_date,
    ls.days_since_last_purchase
FROM current_profile       cp
LEFT JOIN tier_change_count  tc ON cp.customer_id = tc.customer_id
LEFT JOIN tier_journey       tj ON cp.customer_id = tj.customer_id
LEFT JOIN lifetime_stats     ls ON cp.customer_id = ls.customer_id
ORDER BY ls.lifetime_spend DESC NULLS LAST
