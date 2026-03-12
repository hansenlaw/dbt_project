{{ config(materialized='table') }}

/*
  Untuk: Store Manager
  Isi  : KPI performa tiap toko per bulan —
         revenue, jumlah order, unique customer, avg order value, dan rank
  Kegunaan:
    - Monitor target bulanan per toko
    - Bandingkan performa antar toko (ranking)
    - Deteksi toko yang perlu perhatian (revenue drop)
*/

WITH sales_with_store AS (

    SELECT
        fs.order_id,
        fs.order_date,
        fs.customer_id,
        fs.product_id,
        fs.quantity,
        fs.total_amount,
        wis.initial_store_code,           -- stable store ID meski kode toko berubah
        wis.store_code AS current_code    -- kode toko saat transaksi terjadi
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('working_initial_store') }} wis
        ON  fs.store_code = wis.store_code
        AND fs.order_date >= wis.start_date
        AND fs.order_date <  wis.end_date

),

monthly AS (

    SELECT
        initial_store_code,
        TO_CHAR(order_date, 'YYYY-MM')        AS month,
        COUNT(DISTINCT order_id)              AS total_orders,
        COUNT(DISTINCT customer_id)           AS unique_customers,
        SUM(total_amount)                     AS total_revenue,
        SUM(quantity)                         AS total_qty_sold,
        ROUND(AVG(total_amount), 2)           AS avg_order_value
    FROM sales_with_store
    GROUP BY 1, 2

),

with_rank AS (

    SELECT
        *,
        RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) AS revenue_rank,
        SUM(total_revenue) OVER (PARTITION BY initial_store_code
                                 ORDER BY month
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                )                                    AS cumulative_revenue
    FROM monthly

)

SELECT
    initial_store_code,
    month,
    total_orders,
    unique_customers,
    total_qty_sold,
    total_revenue,
    avg_order_value,
    revenue_rank,
    cumulative_revenue
FROM with_rank
ORDER BY month, revenue_rank
