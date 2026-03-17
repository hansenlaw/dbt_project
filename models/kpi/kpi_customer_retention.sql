{{ config(materialized='table') }}

/*
  KPI: Customer Acquisition & Retention — Monthly
  Reads from : fact_sales
  Audience   : CRM / Growth team
  Metrics    :
    - total_buyers              → unique paying customers this month
    - new_customers             → first-ever purchase in this month
    - returning_customers       → bought in this AND a previous month
    - retained_from_prev_month  → of last month's buyers, how many came back
    - retention_rate_pct        → retained / prev_month_buyers × 100
    - churn_rate_pct            → 100 − retention_rate_pct
    - new_customer_pct          → acquisition health (% of buyers who are new)
    - buyers_mom_growth_pct     → buyer base growth trend
*/

WITH monthly_buyers AS (

    -- one row per (month, customer) — the raw base for all retention logic
    SELECT DISTINCT
        TO_CHAR(order_date, 'YYYY-MM') AS month,
        customer_id
    FROM {{ ref('fact_sales') }}

),

first_purchase AS (

    -- each customer's very first purchase month
    SELECT
        customer_id,
        MIN(month) AS first_month
    FROM monthly_buyers
    GROUP BY 1

),

monthly_stats AS (

    SELECT
        mb.month,
        COUNT(DISTINCT mb.customer_id)                                                          AS total_buyers,
        COUNT(DISTINCT CASE WHEN fp.first_month = mb.month THEN mb.customer_id END)             AS new_customers,
        COUNT(DISTINCT CASE WHEN fp.first_month < mb.month  THEN mb.customer_id END)            AS returning_customers
    FROM monthly_buyers mb
    JOIN first_purchase fp ON mb.customer_id = fp.customer_id
    GROUP BY 1

),

retained AS (

    -- buyers who bought this month AND also bought the immediately previous month
    SELECT
        curr.month,
        COUNT(DISTINCT curr.customer_id) AS retained_count
    FROM monthly_buyers curr
    INNER JOIN monthly_buyers prev
        ON  curr.customer_id = prev.customer_id
        AND prev.month = TO_CHAR(
                (curr.month || '-01')::date - INTERVAL '1 month',
                'YYYY-MM'
            )
    GROUP BY 1

),

base AS (

    SELECT
        ms.month,
        ms.total_buyers,
        ms.new_customers,
        ms.returning_customers,
        COALESCE(r.retained_count, 0)                               AS retained_from_prev_month,
        LAG(ms.total_buyers) OVER (ORDER BY ms.month)               AS prev_month_buyers
    FROM monthly_stats ms
    LEFT JOIN retained r ON r.month = ms.month

)

SELECT
    month,
    total_buyers,
    new_customers,
    returning_customers,
    retained_from_prev_month,
    prev_month_buyers,

    -- Retention rate: of last month's buyers, what % came back this month?
    CASE
        WHEN prev_month_buyers IS NULL OR prev_month_buyers = 0 THEN NULL
        ELSE ROUND(
            retained_from_prev_month::numeric / prev_month_buyers * 100
        , 2)
    END                                                             AS retention_rate_pct,

    -- Churn rate: complement of retention
    CASE
        WHEN prev_month_buyers IS NULL OR prev_month_buyers = 0 THEN NULL
        ELSE ROUND(
            (1 - retained_from_prev_month::numeric / prev_month_buyers) * 100
        , 2)
    END                                                             AS churn_rate_pct,

    -- New customer ratio (acquisition contribution)
    ROUND(
        new_customers::numeric / NULLIF(total_buyers, 0) * 100
    , 2)                                                            AS new_customer_pct,

    -- MoM buyer base growth
    CASE
        WHEN prev_month_buyers IS NULL OR prev_month_buyers = 0 THEN NULL
        ELSE ROUND(
            (total_buyers - prev_month_buyers)::numeric / prev_month_buyers * 100
        , 2)
    END                                                             AS buyers_mom_growth_pct

FROM base
ORDER BY month
