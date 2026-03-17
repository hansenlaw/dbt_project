{{ config(materialized='table') }}

/*
  Executive Summary: Monthly KPI Dashboard
  ──────────────────────────────────────────────────────────────────────────────
  Joins all 5 individual KPI models into one wide table.
  One row per month — single source of truth for C-level reporting.

  Consumer  : CEO / COO / Leadership team
  Refresh   : After every full dbt build (dbt run --select tag:kpi)

  Column sections:
    [A] Period
    [B] Revenue
    [C] Orders & Basket
    [D] Customer & Retention
    [E] Store Network
    [F] Product Portfolio
  ──────────────────────────────────────────────────────────────────────────────
*/

WITH base AS (

    SELECT
        COALESCE(r.month, o.month, cr.month, s.month, p.month)  AS month,

        -- ── [B] REVENUE ──────────────────────────────────────────────────────
        r.total_revenue,
        r.prev_month_revenue,
        r.revenue_mom_growth_pct,
        r.revenue_3m_rolling_avg,
        r.cumulative_revenue,
        r.revenue_share_pct,

        -- ── [C] ORDERS & BASKET ──────────────────────────────────────────────
        o.total_orders,
        o.unique_buyers,
        o.active_stores,
        o.distinct_products,
        o.total_qty_sold,
        o.avg_order_value,
        o.avg_items_per_order,
        o.orders_mom_growth_pct,
        o.aov_mom_change_pct,
        o.avg_revenue_per_store,
        o.avg_orders_per_buyer,

        -- ── [D] CUSTOMER & RETENTION ─────────────────────────────────────────
        cr.new_customers,
        cr.returning_customers,
        cr.retained_from_prev_month,
        cr.prev_month_buyers,
        cr.retention_rate_pct,
        cr.churn_rate_pct,
        cr.new_customer_pct,
        cr.buyers_mom_growth_pct,

        -- ── [E] STORE NETWORK ────────────────────────────────────────────────
        s.active_store_count,
        s.avg_revenue_per_store                                 AS store_avg_revenue,
        s.top_store_code,
        s.top_store_revenue,
        s.top_store_revenue_share_pct,
        s.bottom_store_code,
        s.bottom_store_revenue,
        s.revenue_gap_top_bottom,
        s.top3_concentration_pct,
        s.market_revenue_mom_growth_pct,

        -- ── [F] PRODUCT PORTFOLIO ────────────────────────────────────────────
        p.distinct_products_sold,
        p.avg_revenue_per_product,
        p.top_product_by_revenue,
        p.top_product_revenue,
        p.top_product_revenue_share_pct,
        p.top_product_by_qty,
        p.product_diversity_index,
        p.product_revenue_mom_growth_pct

    FROM            {{ ref('kpi_revenue') }}            r
    FULL JOIN       {{ ref('kpi_orders') }}             o   ON o.month  = r.month
    FULL JOIN       {{ ref('kpi_customer_retention') }} cr  ON cr.month = COALESCE(r.month, o.month)
    FULL JOIN       {{ ref('kpi_store') }}              s   ON s.month  = COALESCE(r.month, o.month, cr.month)
    FULL JOIN       {{ ref('kpi_product') }}            p   ON p.month  = COALESCE(r.month, o.month, cr.month, s.month)

)

SELECT * FROM base
ORDER BY month
