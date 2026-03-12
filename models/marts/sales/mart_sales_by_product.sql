{{ config(materialized='table') }}

/*
  Untuk: Tim Sales
  Isi  : Performa tiap produk — revenue, qty terjual, jumlah transaksi,
         harga rata-rata, dan ranking
  Kegunaan:
    - Identifikasi produk terlaris (untuk prioritas stok & promosi)
    - Deteksi produk slow-moving
    - Analisa apakah harga produk konsisten antar toko
*/

WITH product_stats AS (

    SELECT
        product_id,
        TO_CHAR(order_date, 'YYYY-MM')  AS month,
        COUNT(DISTINCT order_id)        AS total_transactions,
        COUNT(DISTINCT customer_id)     AS unique_buyers,
        COUNT(DISTINCT store_code)      AS sold_in_stores,
        SUM(quantity)                   AS total_qty_sold,
        SUM(total_amount)               AS total_revenue,
        ROUND(AVG(unit_price), 2)       AS avg_unit_price,
        MIN(unit_price)                 AS min_unit_price,
        MAX(unit_price)                 AS max_unit_price
    FROM {{ ref('fact_sales') }}
    GROUP BY 1, 2

),

with_rank AS (

    SELECT
        *,
        RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC)  AS revenue_rank,
        RANK() OVER (PARTITION BY month ORDER BY total_qty_sold DESC) AS qty_rank
    FROM product_stats

)

SELECT
    product_id,
    month,
    total_transactions,
    unique_buyers,
    sold_in_stores,
    total_qty_sold,
    total_revenue,
    avg_unit_price,
    min_unit_price,
    max_unit_price,
    revenue_rank,
    qty_rank
FROM with_rank
ORDER BY month, revenue_rank
