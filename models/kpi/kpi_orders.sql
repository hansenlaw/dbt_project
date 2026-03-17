{{ config(
    materialized = 'incremental',
    unique_key   = 'month'
) }}

/*
  KPI: Order Volume & Basket Quality — Monthly
  Reads from : fact_sales
  Audience   : Sales / Operations
  Metrics    :
    - total_orders, unique_buyers, active_stores, distinct_products
    - avg_order_value, avg_items_per_order, avg_orders_per_buyer
    - orders_mom_growth_pct     → order volume trend
    - aov_mom_change_pct        → basket size trend
    - avg_revenue_per_store     → store productivity

  Materialization note:
    Incremental (unique_key = 'month'). On the first run (or --full-refresh) the
    entire history is built with standard window functions (LAG). On subsequent
    runs only new months are loaded from fact_sales, and the previous-month
    values are looked up from {{ this }} via correlated subquery — so window
    functions are not needed and row-level correctness is preserved.
*/

WITH monthly AS (

    SELECT
        TO_CHAR(order_date, 'YYYY-MM')                                          AS month,
        COUNT(DISTINCT order_id)                                                AS total_orders,
        COUNT(DISTINCT customer_id)                                             AS unique_buyers,
        COUNT(DISTINCT store_code)                                              AS active_stores,
        COUNT(DISTINCT product_id)                                              AS distinct_products,
        SUM(quantity)                                                           AS total_qty_sold,
        SUM(total_amount)                                                       AS total_revenue,
        ROUND(AVG(total_amount), 2)                                             AS avg_order_value,
        ROUND(
            SUM(quantity)::numeric / NULLIF(COUNT(DISTINCT order_id), 0)
        , 2)                                                                    AS avg_items_per_order
    FROM {{ ref('fact_sales') }}

    {% if is_incremental() %}
    -- Only process months that don't exist yet in the table
    WHERE TO_CHAR(order_date, 'YYYY-MM') > (SELECT MAX(month) FROM {{ this }})
    {% endif %}

    GROUP BY 1

),

with_lag AS (

    SELECT
        m.*,

        {% if is_incremental() %}
        -- In incremental mode: look up previous month values from the
        -- already-materialized table. Avoids window functions over a
        -- single-row CTE that would always produce NULL.
        (
            SELECT t.total_orders
            FROM   {{ this }} t
            WHERE  t.month = TO_CHAR(
                       (m.month || '-01')::date - INTERVAL '1 month',
                       'YYYY-MM'
                   )
        )                                                                       AS prev_month_orders,
        (
            SELECT t.avg_order_value
            FROM   {{ this }} t
            WHERE  t.month = TO_CHAR(
                       (m.month || '-01')::date - INTERVAL '1 month',
                       'YYYY-MM'
                   )
        )                                                                       AS prev_month_aov,
        (
            SELECT t.unique_buyers
            FROM   {{ this }} t
            WHERE  t.month = TO_CHAR(
                       (m.month || '-01')::date - INTERVAL '1 month',
                       'YYYY-MM'
                   )
        )                                                                       AS prev_month_buyers

        {% else %}
        -- In full refresh mode: standard window functions over complete history
        LAG(total_orders)    OVER (ORDER BY month)                              AS prev_month_orders,
        LAG(avg_order_value) OVER (ORDER BY month)                              AS prev_month_aov,
        LAG(unique_buyers)   OVER (ORDER BY month)                              AS prev_month_buyers
        {% endif %}

    FROM monthly m

)

SELECT
    month,
    total_orders,
    unique_buyers,
    active_stores,
    distinct_products,
    total_qty_sold,
    total_revenue,
    avg_order_value,
    avg_items_per_order,

    -- Order volume MoM
    prev_month_orders,
    CASE
        WHEN prev_month_orders IS NULL OR prev_month_orders = 0 THEN NULL
        ELSE ROUND(
            (total_orders - prev_month_orders)::numeric
            / prev_month_orders * 100
        , 2)
    END                                                 AS orders_mom_growth_pct,

    -- Average Order Value MoM
    prev_month_aov,
    CASE
        WHEN prev_month_aov IS NULL OR prev_month_aov = 0 THEN NULL
        ELSE ROUND(
            (avg_order_value - prev_month_aov)
            / prev_month_aov * 100
        , 2)
    END                                                 AS aov_mom_change_pct,

    -- Store productivity (revenue per active store)
    ROUND(
        total_revenue::numeric / NULLIF(active_stores, 0)
    , 2)                                                AS avg_revenue_per_store,

    -- Purchase frequency (orders per buyer — loyalty proxy)
    ROUND(
        total_orders::numeric / NULLIF(unique_buyers, 0)
    , 2)                                                AS avg_orders_per_buyer

FROM with_lag
ORDER BY month
