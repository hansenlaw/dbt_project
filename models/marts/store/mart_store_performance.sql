{{ config(materialized='table') }}

/*
  For: Store Managers
  Contains: Monthly store KPIs —
            revenue, order count, unique customers, avg order value, and ranking
  Use cases:
    - Monitor monthly targets per store
    - Benchmark performance across stores (ranking)
    - Detect stores that need attention (revenue drop)
*/

WITH sales_with_store AS (

    SELECT
        fs.order_id,
        fs.order_date,
        fs.customer_id,
        fs.product_id,
        fs.quantity,
        fs.total_amount,
        wis.initial_store_code,           -- stable store ID even when store code changes
        wis.store_code AS current_code    -- store code at the time of the transaction
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
