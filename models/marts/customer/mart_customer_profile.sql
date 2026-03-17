{{ config(materialized='table') }}

/*
  For: Marketing / CRM team
  Contains: Full customer directory — current profile, active loyalty tier,
            tier change history (SCD2), and lifetime transaction statistics
  Use cases:
    - View complete customer profile with active tier
    - Track customer tier progression history (Bronze → Silver → Gold)
    - View lifetime value contribution per customer
    - Base for campaign targeting by tier and location

  Note: requires dbt snapshot dim_customer to be run before dbt run
        (`dbt snapshot --var 'raw_data_date: ...'`)
*/

WITH all_versions AS (

    -- single scan of dim_customer; split into current profile and history below
    -- cleaning applied here so all downstream CTEs get standardised values
    SELECT
        {{ clean_id('customer_id') }}       AS customer_id,
        {{ clean_name('full_name') }}       AS full_name,
        {{ clean_email('email') }}          AS email,
        phone,
        {{ clean_name('city') }}            AS city,
        {{ clean_name('province') }}        AS province,
        registration_date,
        tier,
        dbt_valid_from,
        dbt_valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY dbt_valid_from
        ) AS version_number
    FROM {{ ref('dim_customer') }}

),

current_profile AS (

    -- most recent row (dbt_valid_to IS NULL = still active)
    SELECT
        customer_id,
        full_name,
        email,
        phone,
        city,
        province,
        registration_date,
        tier       AS current_tier,
        dbt_valid_from AS tier_since
    FROM all_versions
    WHERE dbt_valid_to IS NULL

),

tier_change_count AS (

    SELECT
        customer_id,
        COUNT(*) - 1 AS total_tier_upgrades   -- first version is not a "change"
    FROM all_versions
    GROUP BY 1

),

tier_journey AS (

    -- chronological tier progression summary: Bronze → Silver → Gold
    SELECT
        customer_id,
        STRING_AGG(tier, ' → ' ORDER BY dbt_valid_from) AS tier_journey
    FROM all_versions
    GROUP BY 1

),

lifetime_stats AS (

    SELECT
        customer_id,
        COUNT(DISTINCT order_id)           AS lifetime_orders,
        SUM(total_amount)                  AS lifetime_spend,
        MIN(order_date)                    AS first_purchase_date,
        MAX(order_date)                    AS last_purchase_date,
        CURRENT_DATE - MAX(order_date)     AS days_since_last_purchase
    FROM {{ ref('fact_sales') }}
    GROUP BY 1

)

SELECT
    cp.customer_id,
    cp.full_name,
    cp.email,
    cp.phone,
    cp.city,
    cp.province,
    cp.registration_date,
    CURRENT_DATE - cp.registration_date               AS days_as_customer,
    cp.current_tier,
    cp.tier_since,
    COALESCE(tc.total_tier_upgrades, 0)               AS total_tier_upgrades,
    tj.tier_journey,
    COALESCE(ls.lifetime_orders, 0)                   AS lifetime_orders,
    COALESCE(ls.lifetime_spend, 0)                    AS lifetime_spend,
    ls.first_purchase_date,
    ls.last_purchase_date,
    ls.days_since_last_purchase
FROM current_profile       cp
LEFT JOIN tier_change_count  tc ON cp.customer_id = tc.customer_id
LEFT JOIN tier_journey       tj ON cp.customer_id = tj.customer_id
LEFT JOIN lifetime_stats     ls ON cp.customer_id = ls.customer_id
