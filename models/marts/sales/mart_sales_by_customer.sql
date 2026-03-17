{{ config(
    materialized = 'table',
    post_hook    = "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_customer ON {{ this }} (customer_id)"
) }}

/*
  For: Sales team
  Contains: Customer value profiles — total spend, frequency, recency,
            favorite product, and RFM behavioral segmentation
  Use cases:
    - Identify high-value customers (for retention and loyalty programs)
    - Identify customers who have not purchased recently (churn risk)
    - Base for segmentation in promotional campaigns
*/

WITH order_level AS (

    -- single scan of fact_sales; shared by customer_base and customer_product_qty
    SELECT
        customer_id,
        product_id,
        store_code,
        order_id,
        order_date,
        total_amount,
        quantity
    FROM {{ ref('fact_sales') }}

),

customer_base AS (

    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                            AS total_orders,
        COUNT(DISTINCT store_code)                          AS stores_visited,
        COUNT(DISTINCT product_id)                          AS unique_products_bought,
        SUM(quantity)                                       AS total_qty,
        SUM(total_amount)                                   AS total_spend,
        ROUND(AVG(total_amount), 2)                         AS avg_order_value,
        MIN(order_date)                                     AS first_purchase_date,
        MAX(order_date)                                     AS last_purchase_date,
        MAX(order_date) - MIN(order_date)                   AS customer_lifespan_days,
        CURRENT_DATE - MAX(order_date)                      AS days_since_last_purchase
    FROM order_level
    GROUP BY 1

),

customer_product_qty AS (

    SELECT
        customer_id,
        product_id,
        SUM(quantity) AS total_qty
    FROM order_level
    GROUP BY 1, 2

),

favorite_product AS (

    -- product with highest total quantity purchased per customer
    SELECT DISTINCT ON (customer_id)
        customer_id,
        product_id AS favorite_product_id,
        total_qty  AS qty_of_fav_product
    FROM customer_product_qty
    ORDER BY customer_id, total_qty DESC

),

rfm_score AS (

    SELECT
        customer_id,
        -- Recency: fewer days since last purchase = better (score 3)
        CASE
            WHEN days_since_last_purchase <= 30  THEN 3
            WHEN days_since_last_purchase <= 90  THEN 2
            ELSE 1
        END AS recency_score,
        -- Frequency: more orders = better
        CASE
            WHEN total_orders >= 5 THEN 3
            WHEN total_orders >= 2 THEN 2
            ELSE 1
        END AS frequency_score,
        -- Monetary: higher spend = better
        CASE
            WHEN total_spend >= 300000 THEN 3
            WHEN total_spend >= 100000 THEN 2
            ELSE 1
        END AS monetary_score
    FROM customer_base

),

rfm_segment AS (

    SELECT
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        recency_score + frequency_score + monetary_score AS rfm_total,
        CASE
            WHEN recency_score + frequency_score + monetary_score >= 8 THEN 'Champions'
            WHEN recency_score + frequency_score + monetary_score >= 6 THEN 'Loyal'
            WHEN recency_score >= 2 AND frequency_score = 1            THEN 'Promising'
            WHEN recency_score = 1 AND frequency_score >= 2            THEN 'At Risk'
            ELSE 'Need Attention'
        END AS customer_segment
    FROM rfm_score

)

SELECT
    cb.customer_id,
    cb.total_orders,
    cb.stores_visited,
    cb.unique_products_bought,
    cb.total_qty,
    cb.total_spend,
    cb.avg_order_value,
    cb.first_purchase_date,
    cb.last_purchase_date,
    cb.customer_lifespan_days,
    cb.days_since_last_purchase,
    fp.favorite_product_id,
    rs.recency_score,
    rs.frequency_score,
    rs.monetary_score,
    rs.customer_segment
FROM customer_base       cb
LEFT JOIN favorite_product fp ON cb.customer_id = fp.customer_id
LEFT JOIN rfm_segment      rs ON cb.customer_id = rs.customer_id
