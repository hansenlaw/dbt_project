{{ config(materialized='table') }}

/*
  KPI: Store Network Performance — Monthly
  Reads from : mart_store_performance
  Audience   : Operations / Store Management
  Metrics    :
    - active_store_count                  → stores with sales this month
    - total_market_revenue                → network-wide revenue
    - avg_revenue_per_store               → store productivity benchmark
    - top_store_code / revenue            → this month's best performer
    - bottom_store_code / revenue         → this month's lowest performer
    - revenue_gap_top_bottom              → spread between best and worst
    - top3_concentration_pct              → revenue held by top-3 stores (Pareto proxy)
    - market_revenue_mom_growth_pct       → overall network MoM trend
*/

WITH store_monthly AS (

    SELECT
        month,
        initial_store_code,
        total_revenue,
        total_orders,
        unique_customers,
        avg_order_value,
        revenue_rank
    FROM {{ ref('mart_store_performance') }}

),

monthly_totals AS (

    SELECT
        month,
        COUNT(DISTINCT initial_store_code)                  AS active_store_count,
        SUM(total_revenue)                                  AS total_market_revenue,
        SUM(total_orders)                                   AS market_total_orders,
        SUM(unique_customers)                               AS market_unique_customers,
        ROUND(AVG(total_revenue), 2)                        AS avg_revenue_per_store,
        MIN(total_revenue)                                  AS min_store_revenue,
        MAX(total_revenue)                                  AS max_store_revenue
    FROM store_monthly
    GROUP BY 1

),

top_store AS (

    -- the highest-revenue store for each month
    SELECT DISTINCT ON (month)
        month,
        initial_store_code  AS top_store_code,
        total_revenue       AS top_store_revenue,
        total_orders        AS top_store_orders,
        unique_customers    AS top_store_unique_customers
    FROM store_monthly
    ORDER BY month, total_revenue DESC

),

bottom_store AS (

    -- the lowest-revenue store for each month
    SELECT DISTINCT ON (month)
        month,
        initial_store_code  AS bottom_store_code,
        total_revenue       AS bottom_store_revenue
    FROM store_monthly
    ORDER BY month, total_revenue ASC

),

top3_concentration AS (

    -- share of revenue held by the top-3 stores (Pareto proxy for concentration)
    SELECT
        month,
        SUM(total_revenue) AS top3_revenue
    FROM (
        SELECT
            month,
            total_revenue,
            RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) AS rk
        FROM store_monthly
    ) ranked
    WHERE rk <= 3
    GROUP BY 1

),

with_lag AS (

    SELECT
        mt.*,
        ts.top_store_code,
        ts.top_store_revenue,
        ts.top_store_orders,
        ts.top_store_unique_customers,
        bs.bottom_store_code,
        bs.bottom_store_revenue,
        t3.top3_revenue,
        LAG(mt.total_market_revenue) OVER (ORDER BY mt.month) AS prev_month_market_revenue
    FROM monthly_totals mt
    JOIN top_store ts          ON ts.month = mt.month
    JOIN bottom_store bs       ON bs.month = mt.month
    JOIN top3_concentration t3 ON t3.month = mt.month

)

SELECT
    month,
    active_store_count,
    total_market_revenue,
    market_total_orders,
    market_unique_customers,
    avg_revenue_per_store,
    min_store_revenue,
    max_store_revenue,

    -- Top performer
    top_store_code,
    top_store_revenue,
    top_store_orders,
    top_store_unique_customers,
    ROUND(
        top_store_revenue::numeric / NULLIF(total_market_revenue, 0) * 100
    , 2)                                                            AS top_store_revenue_share_pct,

    -- Bottom performer
    bottom_store_code,
    bottom_store_revenue,

    -- Performance spread (risk indicator — high gap = uneven network)
    ROUND(top_store_revenue - bottom_store_revenue, 2)             AS revenue_gap_top_bottom,

    -- Market concentration (top-3 Pareto proxy)
    top3_revenue,
    ROUND(
        top3_revenue::numeric / NULLIF(total_market_revenue, 0) * 100
    , 2)                                                            AS top3_concentration_pct,

    -- Network MoM revenue trend
    prev_month_market_revenue,
    CASE
        WHEN prev_month_market_revenue IS NULL OR prev_month_market_revenue = 0 THEN NULL
        ELSE ROUND(
            (total_market_revenue - prev_month_market_revenue)::numeric
            / prev_month_market_revenue * 100
        , 2)
    END                                                             AS market_revenue_mom_growth_pct

FROM with_lag
ORDER BY month
