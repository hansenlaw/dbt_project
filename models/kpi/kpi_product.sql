{{ config(materialized='table') }}

/*
  KPI: Product Portfolio Performance — Monthly
  Reads from : mart_sales_by_product
  Audience   : Sales / Merchandising
  Metrics    :
    - distinct_products_sold          → portfolio breadth
    - total_product_revenue           → portfolio total
    - avg_revenue_per_product         → portfolio efficiency
    - top_product_by_revenue          → this month's revenue champion
    - top_product_revenue_share_pct   → concentration risk (single product dominance)
    - top_product_by_qty              → volume leader (may differ from revenue leader)
    - product_diversity_index         → 0 = one product dominates, 1 = perfect spread
    - product_revenue_mom_growth_pct  → portfolio growth trend
*/

WITH product_monthly AS (

    SELECT
        month,
        product_id,
        total_qty_sold,
        total_revenue,
        total_transactions,
        unique_buyers,
        sold_in_stores,
        avg_unit_price,
        min_unit_price,
        max_unit_price,
        revenue_rank,
        qty_rank
    FROM {{ ref('mart_sales_by_product') }}

),

monthly_totals AS (

    SELECT
        month,
        COUNT(DISTINCT product_id)                              AS distinct_products_sold,
        SUM(total_revenue)                                      AS total_product_revenue,
        SUM(total_qty_sold)                                     AS total_qty_sold,
        SUM(total_transactions)                                 AS total_transactions,
        ROUND(AVG(total_revenue), 2)                            AS avg_revenue_per_product
    FROM product_monthly
    GROUP BY 1

),

top_by_revenue AS (

    -- product with highest revenue this month
    SELECT DISTINCT ON (month)
        month,
        product_id                      AS top_product_by_revenue,
        total_revenue                   AS top_product_revenue,
        total_qty_sold                  AS top_product_qty_sold,
        unique_buyers                   AS top_product_unique_buyers,
        sold_in_stores                  AS top_product_sold_in_stores
    FROM product_monthly
    ORDER BY month, total_revenue DESC

),

top_by_qty AS (

    -- product with highest quantity sold this month (may differ from revenue leader)
    SELECT DISTINCT ON (month)
        month,
        product_id                      AS top_product_by_qty,
        total_qty_sold                  AS top_product_qty
    FROM product_monthly
    ORDER BY month, total_qty_sold DESC

),

with_lag AS (

    SELECT
        mt.*,
        tr.top_product_by_revenue,
        tr.top_product_revenue,
        tr.top_product_qty_sold,
        tr.top_product_unique_buyers,
        tr.top_product_sold_in_stores,
        tq.top_product_by_qty,
        tq.top_product_qty,
        LAG(mt.distinct_products_sold)    OVER (ORDER BY mt.month) AS prev_month_product_count,
        LAG(mt.total_product_revenue)     OVER (ORDER BY mt.month) AS prev_month_product_revenue
    FROM monthly_totals mt
    JOIN top_by_revenue tr ON tr.month = mt.month
    JOIN top_by_qty     tq ON tq.month = mt.month

)

SELECT
    month,
    distinct_products_sold,
    total_product_revenue,
    total_qty_sold,
    total_transactions,
    avg_revenue_per_product,

    -- Top product by revenue
    top_product_by_revenue,
    top_product_revenue,
    top_product_qty_sold,
    top_product_unique_buyers,
    top_product_sold_in_stores,
    ROUND(
        top_product_revenue::numeric / NULLIF(total_product_revenue, 0) * 100
    , 2)                                                            AS top_product_revenue_share_pct,

    -- Top product by quantity (volume leader vs revenue leader may reveal margin gaps)
    top_product_by_qty,
    top_product_qty,

    -- Product diversity index: 0 = monopoly, ~1 = perfectly distributed
    ROUND(
        (1 - top_product_revenue::numeric / NULLIF(total_product_revenue, 0))::numeric
    , 4)                                                            AS product_diversity_index,

    -- MoM product portfolio changes
    prev_month_product_count,
    distinct_products_sold - COALESCE(prev_month_product_count, 0) AS product_count_mom_change,

    prev_month_product_revenue,
    CASE
        WHEN prev_month_product_revenue IS NULL OR prev_month_product_revenue = 0 THEN NULL
        ELSE ROUND(
            (total_product_revenue - prev_month_product_revenue)::numeric
            / prev_month_product_revenue * 100
        , 2)
    END                                                             AS product_revenue_mom_growth_pct

FROM with_lag
ORDER BY month
