-- ============================================================
-- Diagnostic Queries — io_testing Pipeline
-- Run these in sequence to verify each layer is correct.
-- ============================================================

-- ── SOURCE TABLES ──────────────────────────────────────────

-- How many batches and rows per batch?
SELECT partition_time::date, COUNT(*) AS rows
FROM public.src_sales
GROUP BY 1 ORDER BY 1;

SELECT partition_time::date, COUNT(*) AS rows
FROM public.src_store
GROUP BY 1 ORDER BY 1;

SELECT partition_time::date, COUNT(*) AS rows
FROM public.src_customer
GROUP BY 1 ORDER BY 1;

-- ── INTERMEDIATE ───────────────────────────────────────────

-- fact_sales: total rows and date range
SELECT COUNT(*) AS total_rows, MIN(order_date), MAX(order_date)
FROM public.intermediate__fact_sales;

-- working_initial_store: SCD2 entries
SELECT * FROM public.intermediate__working_initial_store
ORDER BY initial_store_code, start_date;

-- ── KPI LAYER ──────────────────────────────────────────────

-- Row counts per KPI model
SELECT 'kpi_revenue'            AS model, COUNT(*) AS rows FROM public_marts_kpi.kpi_revenue
UNION ALL
SELECT 'kpi_orders',             COUNT(*)           FROM public_marts_kpi.kpi_orders
UNION ALL
SELECT 'kpi_customer_retention', COUNT(*)           FROM public_marts_kpi.kpi_customer_retention
UNION ALL
SELECT 'kpi_store',              COUNT(*)           FROM public_marts_kpi.kpi_store
UNION ALL
SELECT 'kpi_product',            COUNT(*)           FROM public_marts_kpi.kpi_product
UNION ALL
SELECT 'kpi_exec_summary',       COUNT(*)           FROM public_marts_kpi.kpi_exec_summary;

-- Retention rate check (should be NULL for first month, value for subsequent)
SELECT month, total_buyers, new_customers, retained_from_prev_month,
       prev_month_buyers, retention_rate_pct, churn_rate_pct
FROM public_marts_kpi.kpi_customer_retention
ORDER BY month;

-- Executive Summary: all months
SELECT * FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
