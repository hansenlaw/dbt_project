{{ config(materialized='table') }}

/*
  KPI: Revenue Performance — Monthly
  Reads from : fact_sales
  Audience   : Executive / Finance
  Metrics    :
    - total_revenue, total_orders, unique_buyers, total_qty_sold
    - prev_month_revenue        → MoM baseline
    - revenue_mom_growth_pct    → % change vs previous month
    - revenue_3m_rolling_avg    → 3-month smoothed trend line
    - cumulative_revenue        → all-time running total
    - revenue_share_pct         → this month as % of all-time total
*/

WITH monthly AS (

    SELECT
        TO_CHAR(order_date, 'YYYY-MM')          AS month,
        COUNT(DISTINCT order_id)                AS total_orders,
        COUNT(DISTINCT customer_id)             AS unique_buyers,
        SUM(total_amount)                       AS total_revenue,
        SUM(quantity)                           AS total_qty_sold
    FROM {{ ref('fact_sales') }}
    GROUP BY 1

),

with_lag AS (

    SELECT
        *,
        LAG(total_revenue) OVER (ORDER BY month) AS prev_month_revenue
    FROM monthly

)

SELECT
    month,
    total_orders,
    unique_buyers,
    total_revenue,
    total_qty_sold,

    -- MoM comparison
    prev_month_revenue,

    CASE
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
        ELSE ROUND(
            (total_revenue - prev_month_revenue)
            / prev_month_revenue * 100
        , 2)
    END                                                             AS revenue_mom_growth_pct,

    -- 3-month rolling average (smooths seasonal noise)
    ROUND(
        AVG(total_revenue) OVER (
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
    , 2)                                                            AS revenue_3m_rolling_avg,

    -- All-time cumulative revenue
    SUM(total_revenue) OVER (
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                               AS cumulative_revenue,

    -- This month's share of the total all-time revenue
    ROUND(
        total_revenue::numeric
        / NULLIF(SUM(total_revenue) OVER (), 0) * 100
    , 2)                                                            AS revenue_share_pct

FROM with_lag
ORDER BY month
