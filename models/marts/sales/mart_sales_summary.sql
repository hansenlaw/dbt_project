{{ config(materialized='table') }}

/*
  Untuk: Tim Sales
  Isi  : Ringkasan penjualan harian dan bulanan —
         total revenue, order, qty, dan growth vs periode sebelumnya
  Kegunaan:
    - Monitor target revenue harian/bulanan
    - Deteksi hari/bulan dengan penjualan rendah
    - Hitung growth MoM (month-over-month)
*/

WITH daily AS (

    SELECT
        order_date,
        TO_CHAR(order_date, 'YYYY-MM')   AS month,
        COUNT(DISTINCT order_id)         AS total_orders,
        COUNT(DISTINCT customer_id)      AS unique_customers,
        COUNT(DISTINCT store_code)       AS active_stores,
        SUM(quantity)                    AS total_qty,
        SUM(total_amount)                AS total_revenue,
        ROUND(AVG(total_amount), 2)      AS avg_order_value
    FROM {{ ref('fact_sales') }}
    GROUP BY 1, 2

),

monthly AS (

    SELECT
        month,
        SUM(total_orders)       AS total_orders,
        SUM(unique_customers)   AS unique_customers,
        MAX(active_stores)      AS active_stores,
        SUM(total_qty)          AS total_qty,
        SUM(total_revenue)      AS total_revenue,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        LAG(SUM(total_revenue)) OVER (ORDER BY month) AS prev_month_revenue
    FROM daily
    GROUP BY 1

),

monthly_with_growth AS (

    SELECT
        *,
        CASE
            WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
            ELSE ROUND(
                (total_revenue - prev_month_revenue) / prev_month_revenue * 100,
                2
            )
        END AS revenue_growth_pct
    FROM monthly

)

-- DAILY GRAIN: untuk monitoring harian
SELECT
    'daily'                   AS grain,
    order_date::TEXT          AS period,
    month,
    total_orders,
    unique_customers,
    active_stores,
    total_qty,
    total_revenue,
    avg_order_value,
    NULL::NUMERIC             AS revenue_growth_pct
FROM daily

UNION ALL

-- MONTHLY GRAIN: untuk laporan bulanan + growth
SELECT
    'monthly'                 AS grain,
    month                     AS period,
    month,
    total_orders,
    unique_customers,
    active_stores,
    total_qty,
    total_revenue,
    avg_order_value,
    revenue_growth_pct
FROM monthly_with_growth

ORDER BY grain DESC, period
