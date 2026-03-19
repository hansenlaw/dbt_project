-- ================================================================
-- VERIFY DASHBOARD — io_testing
-- ================================================================
-- Tujuan    : Memastikan setiap angka di Metabase bisa ditelusuri
--             dari data mentah (src_sales) hingga card dashboard.
--
-- Prasyarat (jalankan berurutan):
--   1. psql -f script/setup.sql                  → reset src_sales
--   2. psql -f script/insert.sql                 → isi src_store
--   3. psql -f script/create_dummy_customer.sql  → isi src_customer
--   4. dbt run --full-refresh                    → build semua model
--   5. Metabase: ⚙ → Admin → Databases → Sync   → refresh schema
--
-- Struktur file:
--   SECTION 0  — Alur data raw → dashboard (3 flow utama)
--   SECTION 1  — Sanity check row count semua tabel
--   TAB 1      — Executive Summary         Card #1–#11
--   TAB 2      — Revenue & Orders          Card #12–#20
--   TAB 3      — Customer & Retention      Card #21–#31
--   TAB 4      — Store & Product           Card #32–#42
--   BONUS      — Audit retention & store join
-- ================================================================


-- ================================================================
-- SECTION 0 — ALUR DATA (RAW → DASHBOARD)
-- Verifikasi bahwa angka di dashboard = hasil transformasi dari data mentah
-- ================================================================

-- ──────────────────────────────────────────────────────────────
-- FLOW A: TOTAL REVENUE 3.14M  ←  Card #1 (Tab Executive)
-- ──────────────────────────────────────────────────────────────

-- A-1 — Data mentah di src_sales
SELECT
    partition_time                        AS batch,
    COUNT(*)                              AS row_count,
    SUM(total_amount)                     AS revenue_raw
FROM public.src_sales
GROUP BY 1 ORDER BY 1;
-- Expected:
--   2026-01-01 | 10 | 1 230 000
--   2026-02-01 |  7 |   910 000
--   2026-03-01 |  7 | 1 000 000
--   TOTAL      | 24 | 3 140 000  ← harus muncul di ujung pipeline

-- A-2 — Setelah dbt transform → fact_sales (intermediate)
SELECT
    TO_CHAR(order_date, 'YYYY-MM')        AS month,
    COUNT(*)                              AS orders,
    COUNT(DISTINCT customer_id)           AS unique_buyers,
    SUM(total_amount)                     AS revenue
FROM public.fact_sales
GROUP BY 1 ORDER BY 1;
-- Expected (identik dengan A-1 setelah full-refresh):
--   2026-01 | 10 | 9 | 1 230 000
--   2026-02 |  7 | 7 |   910 000
--   2026-03 |  7 | 7 | 1 000 000

-- A-3 — KPI layer → kpi_revenue
SELECT
    month,
    total_revenue,
    cumulative_revenue,
    revenue_share_pct
FROM public_marts_kpi.kpi_revenue ORDER BY month;
-- Expected:
--   2026-01 | 1 230 000 | 1 230 000 | 39.17%
--   2026-02 |   910 000 | 2 140 000 | 28.98%
--   2026-03 | 1 000 000 | 3 140 000 | 31.85%

-- A-4 — Card #1: Metabase Summarize → Sum of total_revenue
SELECT SUM(total_revenue) AS total_revenue_all_time
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 3 140 000  (= 3.1M yang tampil di Card #1)


-- ──────────────────────────────────────────────────────────────
-- FLOW B: RETENTION RATE 32.54%  ←  Card #5 (Tab Executive)
-- Card #5 = Average of retention_rate_pct (Jan NULL diabaikan)
-- ──────────────────────────────────────────────────────────────

-- B-1 — Siapa saja buyer tiap bulan?
SELECT
    TO_CHAR(order_date, 'YYYY-MM')                                  AS month,
    COUNT(DISTINCT customer_id)                                     AS total_buyers,
    STRING_AGG(DISTINCT customer_id, ', ' ORDER BY customer_id)    AS buyer_list
FROM public.fact_sales
GROUP BY 1 ORDER BY 1;
-- Expected:
--   2026-01 | 9 | CUST001...CUST009
--   2026-02 | 7 | CUST002,CUST003,CUST010,CUST011,CUST012,CUST013,CUST014
--   2026-03 | 7 | CUST002,CUST003,CUST004,CUST005,CUST008,CUST009,CUST010

-- B-2 — Siapa yang retained setiap bulan?
WITH monthly AS (
    SELECT DISTINCT TO_CHAR(order_date, 'YYYY-MM') AS month, customer_id
    FROM public.fact_sales
)
SELECT
    curr.month,
    COUNT(DISTINCT prev.customer_id)                                    AS retained_count,
    STRING_AGG(prev.customer_id, ', ' ORDER BY prev.customer_id)        AS retained_list
FROM monthly curr
LEFT JOIN monthly prev
    ON  curr.customer_id = prev.customer_id
    AND prev.month = TO_CHAR(
            (curr.month || '-01')::date - INTERVAL '1 month', 'YYYY-MM')
WHERE prev.customer_id IS NOT NULL
GROUP BY curr.month ORDER BY curr.month;
-- Expected:
--   2026-02 | 2 | CUST002, CUST003  → 2/9 = 22.22%
--   2026-03 | 3 | CUST002, CUST003, CUST010  → 3/7 = 42.86%

-- B-3 — kpi_customer_retention (sumber Card #5)
SELECT month, retained_from_prev_month, prev_month_buyers,
       retention_rate_pct, churn_rate_pct
FROM public_marts_kpi.kpi_customer_retention ORDER BY month;
-- Expected:
--   2026-01 | 0 | NULL |  NULL  |  NULL
--   2026-02 | 2 |    9 | 22.22  | 77.78
--   2026-03 | 3 |    7 | 42.86  | 57.14

-- B-4 — Card #5: Average of retention_rate_pct (NULL Jan diabaikan Metabase)
SELECT AVG(retention_rate_pct) AS avg_retention_rate
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 32.54  ((22.22 + 42.86) / 2 — Jan NULL tidak dihitung)


-- ──────────────────────────────────────────────────────────────
-- FLOW C: AVG ORDER VALUE  ←  Card #4 (Tab Executive)
-- ──────────────────────────────────────────────────────────────

-- C-1 — AOV per bulan dari fact_sales
SELECT
    TO_CHAR(order_date, 'YYYY-MM')             AS month,
    COUNT(DISTINCT order_id)                   AS total_orders,
    SUM(total_amount)                          AS total_revenue,
    ROUND(AVG(total_amount), 2)                AS avg_order_value
FROM public.fact_sales GROUP BY 1 ORDER BY 1;
-- Expected:
--   2026-01 | 10 | 1 230 000 | 123 000.00
--   2026-02 |  7 |   910 000 | 130 000.00
--   2026-03 |  7 | 1 000 000 | 142 857.14

-- C-2 — Card #4: Average of avg_order_value di Metabase
SELECT ROUND(AVG(avg_order_value), 2) AS avg_aov_overall
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 131 952.38  ((123000 + 130000 + 142857.14) / 3)


-- ================================================================
-- SECTION 1 — SANITY CHECK  (jalankan ini PERTAMA sebelum tab)
-- ================================================================

SELECT 'src_sales'                  AS tabel, COUNT(*) AS rows FROM public.src_sales
UNION ALL SELECT 'fact_sales',               COUNT(*) FROM public.fact_sales
UNION ALL SELECT 'kpi_exec_summary',         COUNT(*) FROM public_marts_kpi.kpi_exec_summary
UNION ALL SELECT 'kpi_revenue',              COUNT(*) FROM public_marts_kpi.kpi_revenue
UNION ALL SELECT 'kpi_orders',               COUNT(*) FROM public_marts_kpi.kpi_orders
UNION ALL SELECT 'kpi_customer_retention',   COUNT(*) FROM public_marts_kpi.kpi_customer_retention
UNION ALL SELECT 'kpi_store',                COUNT(*) FROM public_marts_kpi.kpi_store
UNION ALL SELECT 'kpi_product',              COUNT(*) FROM public_marts_kpi.kpi_product
UNION ALL SELECT 'mart_store_performance',   COUNT(*) FROM public_marts_store.mart_store_performance
UNION ALL SELECT 'mart_store_directory',     COUNT(*) FROM public_marts_store.mart_store_directory
UNION ALL SELECT 'mart_sales_summary',       COUNT(*) FROM public_marts_sales.mart_sales_summary
UNION ALL SELECT 'mart_sales_by_product',    COUNT(*) FROM public_marts_sales.mart_sales_by_product
UNION ALL SELECT 'mart_sales_by_customer',   COUNT(*) FROM public_marts_sales.mart_sales_by_customer;
-- Expected:
--   src_sales              → 24
--   fact_sales             → 24
--   kpi_exec_summary       →  3  (2026-01, 2026-02, 2026-03)
--   kpi_revenue            →  3
--   kpi_orders             →  3
--   kpi_customer_retention →  3
--   kpi_store              →  3
--   kpi_product            →  3
--   mart_store_performance → 15  (5 stores × 3 months)
--   mart_store_directory   →  5  (Y001–Y005)
--   mart_sales_summary     →  3
--   mart_sales_by_product  → 12  (4 products × 3 months)
--   mart_sales_by_customer → 14  (14 unique customers)


-- ================================================================
-- TAB 1 — EXECUTIVE SUMMARY
-- Sumber utama: public_marts_kpi.kpi_exec_summary
-- Cara buat di Metabase: New Question → kpi_exec_summary → Summarize
-- ================================================================

-- ── CARD #1 — Total Revenue ──────────────────────────────────
-- Metabase: Summarize → Sum of total_revenue
SELECT SUM(total_revenue) AS total_revenue
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 3 140 000  (Jan 1.23M + Feb 0.91M + Mar 1.0M = 3.1M)


-- ── CARD #2 — Total Orders ───────────────────────────────────
-- Metabase: Summarize → Sum of total_orders
SELECT SUM(total_orders) AS total_orders
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 24  (10 + 7 + 7)


-- ── CARD #3 — Unique Buyers ──────────────────────────────────
-- Metabase: Summarize → Sum of unique_buyers (penjumlahan per bulan)
SELECT SUM(unique_buyers) AS sum_monthly_buyers
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 23  (9 + 7 + 7)
-- Catatan: ini sum per bulan, bukan distinct customer — CUST002 dihitung 3x jika beli 3 bulan


-- ── CARD #4 — Avg Order Value ────────────────────────────────
-- Metabase: Summarize → Average of avg_order_value
SELECT ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 131 952.38  ((123000 + 130000 + 142857.14) / 3)


-- ── CARD #5 — Retention Rate % ───────────────────────────────
-- Metabase: Summarize → Average of retention_rate_pct
SELECT ROUND(AVG(retention_rate_pct), 2) AS avg_retention_rate_pct
FROM public_marts_kpi.kpi_exec_summary;
-- Expected: 32.54  ((22.22 + 42.86) / 2 — Jan NULL diabaikan)


-- ── CARD #6 — Revenue Trend MoM (Combo Bar+Line) ────────────
-- Metabase: Visualize langsung → Bar (total_revenue) + Line (revenue_3m_rolling_avg)
-- X-axis: month, Metric 1: Sum of total_revenue, Metric 2: Avg of revenue_3m_rolling_avg
SELECT
    month,
    total_revenue,
    revenue_3m_rolling_avg
FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
-- Expected:
--   2026-01 | 1 230 000 | 1 230 000.00
--   2026-02 |   910 000 | 1 070 000.00  ((1230000+910000)/2)
--   2026-03 | 1 000 000 | 1 046 666.67  ((1230000+910000+1000000)/3)


-- ── CARD #7 — New vs Returning Customers (Stacked Bar) ───────
-- Metabase: Visualize → Bar Stacked, X: month, Metric: new_customers + returning_customers
SELECT
    month,
    new_customers,
    returning_customers
FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
-- Expected:
--   2026-01 | 9 | 0   (semua pembeli baru)
--   2026-02 | 5 | 2   (CUST002,CUST003 returning; 5 customer baru)
--   2026-03 | 0 | 7   (semua 7 pembeli Mar pernah beli sebelumnya)


-- ── CARD #8 — Revenue Share by Month (Pie/Donut) ─────────────
-- Metabase: Pie chart, Metric: Sum of total_revenue, Dimension: month
SELECT
    month,
    total_revenue,
    revenue_share_pct
FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
-- Expected (total 3 140 000):
--   2026-01 | 1 230 000 | 39.17%
--   2026-02 |   910 000 | 28.98%
--   2026-03 | 1 000 000 | 31.85%


-- ── CARD #9 — Retention Rate vs Churn Rate (Line) ────────────
-- Metabase: Line chart, X: month, Metric 1: Avg retention_rate_pct, Metric 2: Avg churn_rate_pct
SELECT
    month,
    retention_rate_pct,
    churn_rate_pct
FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
-- Expected:
--   2026-01 |  NULL  |  NULL
--   2026-02 | 22.22  | 77.78
--   2026-03 | 42.86  | 57.14
-- Insight: retention + churn = 100. Trend naik dari 22% → 43% = positif


-- ── CARD #10 — MoM Growth % Revenue vs Orders (Bar) ─────────
-- Metabase: Bar chart, X: month, Metric: revenue_mom_growth_pct + orders_mom_growth_pct
SELECT
    month,
    revenue_mom_growth_pct,
    orders_mom_growth_pct
FROM public_marts_kpi.kpi_exec_summary ORDER BY month;
-- Expected:
--   2026-01 |   NULL |   NULL
--   2026-02 | -26.02 | -30.00  (Feb turun vs Jan)
--   2026-03 |   9.89 |   0.00  (Mar revenue naik, orders stagnan)


-- ── CARD #11 — Full Executive KPI Summary Table ──────────────
-- Metabase: Table view, pilih kolom di bawah, Sort: month DESC
SELECT
    month,
    total_revenue,
    revenue_mom_growth_pct,
    revenue_3m_rolling_avg,
    cumulative_revenue,
    total_orders,
    avg_order_value,
    unique_buyers,
    active_store_count,
    distinct_products_sold,
    new_customers,
    returning_customers,
    retention_rate_pct,
    churn_rate_pct,
    top_store_code,
    top3_concentration_pct,
    top_product_by_revenue,
    product_diversity_index
FROM public_marts_kpi.kpi_exec_summary
ORDER BY month DESC;
-- Expected: 3 baris (2026-03 di atas), Jan retention = NULL


-- ================================================================
-- TAB 2 — REVENUE & ORDERS
-- Sumber: kpi_revenue · kpi_orders · mart_sales_by_product
-- ================================================================

-- ── CARD #12 — Total Revenue (Number) ───────────────────────
-- Metabase: kpi_revenue → Sum of total_revenue
SELECT SUM(total_revenue) AS total_revenue
FROM public_marts_kpi.kpi_revenue;
-- Expected: 3 140 000


-- ── CARD #13 — Cumulative Revenue (Number) ──────────────────
-- Metabase: kpi_revenue → Max of cumulative_revenue
SELECT MAX(cumulative_revenue) AS cumulative_revenue
FROM public_marts_kpi.kpi_revenue;
-- Expected: 3 140 000  (nilai kumulatif bulan terakhir)


-- ── CARD #14 — Avg Order Value (Number) ─────────────────────
-- Metabase: kpi_orders → Average of avg_order_value
SELECT ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM public_marts_kpi.kpi_orders;
-- Expected: 131 952.38


-- ── CARD #15 — Avg Items per Order (Number) ─────────────────
-- Metabase: kpi_orders → Average of avg_items_per_order
SELECT ROUND(AVG(avg_items_per_order), 2) AS avg_items_per_order
FROM public_marts_kpi.kpi_orders;
-- Expected: 2.16  ((2.20 + 2.14 + 2.14) / 3)


-- ── CARD #16 — Revenue + 3M Rolling Avg (Combo Chart) ───────
-- Metabase: kpi_revenue, Bar: total_revenue, Line: revenue_3m_rolling_avg, X: month
SELECT
    month,
    total_revenue,
    revenue_3m_rolling_avg,
    prev_month_revenue,
    revenue_mom_growth_pct
FROM public_marts_kpi.kpi_revenue ORDER BY month;
-- Expected:
--   2026-01 | 1 230 000 | 1 230 000.00 |      NULL |   NULL
--   2026-02 |   910 000 | 1 070 000.00 | 1 230 000 | -26.02
--   2026-03 | 1 000 000 | 1 046 666.67 |   910 000 |   9.89


-- ── CARD #17 — Orders Volume per Bulan (Bar Chart) ───────────
-- Metabase: kpi_orders, Bar: total_orders, X: month
SELECT
    month,
    total_orders,
    orders_mom_growth_pct
FROM public_marts_kpi.kpi_orders ORDER BY month;
-- Expected:
--   2026-01 | 10 |   NULL
--   2026-02 |  7 | -30.00
--   2026-03 |  7 |   0.00


-- ── CARD #18 — Top Products by Revenue (Horizontal Bar) ──────
-- Metabase: mart_sales_by_product, Sum of total_revenue, Grouped by product_id, Sort DESC
SELECT
    product_id,
    SUM(total_revenue)      AS total_revenue_all_time,
    SUM(total_qty_sold)     AS total_qty_all_time
FROM public_marts_sales.mart_sales_by_product
GROUP BY product_id
ORDER BY total_revenue_all_time DESC
LIMIT 10;
-- Expected (4 produk, all-time Jan+Feb+Mar):
--   PRD003 | 1 080 000 | 9   (360k × 3 bulan)
--   PRD001 |   800 000 | 16
--   PRD002 |   750 000 | 10
--   PRD004 |   510 000 | 17


-- ── CARD #19 — AOV vs Avg Items per Order (Line dual-axis) ───
-- Metabase: kpi_orders, Line 1: avg_order_value, Line 2: avg_items_per_order, X: month
SELECT
    month,
    avg_order_value,
    avg_items_per_order,
    aov_mom_change_pct
FROM public_marts_kpi.kpi_orders ORDER BY month;
-- Expected:
--   2026-01 | 123 000.00 | 2.20 |  NULL
--   2026-02 | 130 000.00 | 2.14 |  5.69
--   2026-03 | 142 857.14 | 2.14 |  9.89


-- ── CARD #20 — Product Performance Detail Table ──────────────
-- Metabase: mart_sales_by_product, pilih kolom, Sort: revenue_rank ASC
SELECT
    product_id,
    month,
    total_revenue,
    total_qty_sold,
    unique_buyers,
    avg_unit_price,
    revenue_rank,
    qty_rank
FROM public_marts_sales.mart_sales_by_product
ORDER BY month, revenue_rank;
-- Expected: 12 baris (4 products × 3 months)
-- Mar terbesar: PRD003 360k rank 1, PRD002 300k rank 2, PRD001 250k rank 3, PRD004 90k rank 4


-- ================================================================
-- TAB 3 — CUSTOMER & RETENTION
-- Sumber: kpi_customer_retention · mart_sales_by_customer
-- ================================================================

-- ── CARD #21 — Total Buyers (Number) ────────────────────────
-- Metabase: kpi_customer_retention → Sum of total_buyers
SELECT SUM(total_buyers) AS total_buyers
FROM public_marts_kpi.kpi_customer_retention;
-- Expected: 23  (9 Jan + 7 Feb + 7 Mar = sum per bulan)


-- ── CARD #22 — New Customers (Number) ───────────────────────
-- Metabase: kpi_customer_retention → Sum of new_customers
SELECT SUM(new_customers) AS total_new_customers
FROM public_marts_kpi.kpi_customer_retention;
-- Expected: 14  (9 Jan + 5 Feb + 0 Mar)


-- ── CARD #23 — Retention Rate % (Number) ────────────────────
-- Metabase: kpi_customer_retention → Average of retention_rate_pct
SELECT ROUND(AVG(retention_rate_pct), 2) AS avg_retention_rate_pct
FROM public_marts_kpi.kpi_customer_retention;
-- Expected: 32.54  ((22.22 + 42.86) / 2 — NULL Jan diabaikan)


-- ── CARD #24 — Churn Rate % (Number) ────────────────────────
-- Metabase: kpi_customer_retention → Average of churn_rate_pct
SELECT ROUND(AVG(churn_rate_pct), 2) AS avg_churn_rate_pct
FROM public_marts_kpi.kpi_customer_retention;
-- Expected: 67.46  ((77.78 + 57.14) / 2 — NULL Jan diabaikan)


-- ── CARD #25 — New Customer % (Number) ──────────────────────
-- Metabase: kpi_customer_retention → Average of new_customer_pct
SELECT ROUND(AVG(new_customer_pct), 2) AS avg_new_customer_pct
FROM public_marts_kpi.kpi_customer_retention;
-- Expected: 57.14  ((100.00 + 71.43 + 0.00) / 3)


-- ── CARD #26 — Customer Breakdown per Bulan (Stacked Bar) ────
-- Metabase: kpi_customer_retention, Stacked: new + returning + retained, X: month
SELECT
    month,
    new_customers,
    returning_customers,
    retained_from_prev_month,
    total_buyers
FROM public_marts_kpi.kpi_customer_retention ORDER BY month;
-- Expected:
--   2026-01 | 9 | 0 | 0 | 9
--   2026-02 | 5 | 2 | 2 | 7  (returning = pernah beli; retained = beli bulan sebelumnya)
--   2026-03 | 0 | 7 | 3 | 7  (CUST002,CUST003,CUST010 retained dari Feb)


-- ── CARD #27 — Retention Rate Trend (Line Chart) ─────────────
-- Metabase: kpi_customer_retention, Line: Avg of retention_rate_pct, X: month
SELECT
    month,
    retention_rate_pct,
    prev_month_buyers
FROM public_marts_kpi.kpi_customer_retention ORDER BY month;
-- Expected:
--   2026-01 |  NULL | NULL
--   2026-02 | 22.22 |    9
--   2026-03 | 42.86 |    7  ← trend naik = positif


-- ── CARD #28 — Customer Segment Distribution (Pie) ───────────
-- Metabase: mart_sales_by_customer, Count of customer_id, Grouped by customer_segment
SELECT
    customer_segment,
    COUNT(*) AS customer_count
FROM public_marts_sales.mart_sales_by_customer
GROUP BY 1 ORDER BY 2 DESC;
-- Expected: 5 segmen (Champions/Loyal/Promising/At Risk/Need Attention)
-- Total harus = 14 customers


-- ── CARD #29 — Avg Spend per Segment (Horizontal Bar) ────────
-- Metabase: mart_sales_by_customer, Avg of total_spend, Grouped by customer_segment, Sort DESC
SELECT
    customer_segment,
    COUNT(*)                                AS customer_count,
    ROUND(AVG(total_spend), 0)              AS avg_spend,
    ROUND(AVG(total_orders), 2)             AS avg_orders
FROM public_marts_sales.mart_sales_by_customer
GROUP BY 1 ORDER BY avg_spend DESC;
-- Expected: Champions punya avg_spend tertinggi


-- ── CARD #30 — Buyers MoM Growth (Bar Chart) ─────────────────
-- Metabase: kpi_customer_retention, Bar: Avg of buyers_mom_growth_pct, X: month
SELECT
    month,
    total_buyers,
    buyers_mom_growth_pct
FROM public_marts_kpi.kpi_customer_retention ORDER BY month;
-- Expected:
--   2026-01 | 9 |   NULL
--   2026-02 | 7 | -22.22  (turun dari 9 ke 7)
--   2026-03 | 7 |   0.00  (stagnan)


-- ── CARD #31 — Top 20 Customers by Lifetime Spend (Table) ────
-- Metabase: mart_sales_by_customer, pilih kolom, Sort: total_spend DESC, Limit 20
SELECT
    customer_id,
    total_spend,
    total_orders,
    avg_order_value,
    customer_segment,
    days_since_last_purchase,
    recency_score,
    monetary_score
FROM public_marts_sales.mart_sales_by_customer
ORDER BY total_spend DESC
LIMIT 20;
-- Expected: 14 baris (semua customer, semuanya masuk Limit 20)
-- CUST007 kemungkinan teratas: 1 order senilai 240k → besar untuk 1 transaksi


-- ================================================================
-- TAB 4 — STORE & PRODUCT PERFORMANCE
-- Sumber: kpi_store · kpi_product · mart_store_performance · mart_store_directory
-- ================================================================

-- ── CARD #32 — Active Stores (Number) ───────────────────────
-- Metabase: kpi_store → Max of active_store_count
SELECT MAX(active_store_count) AS active_stores
FROM public_marts_kpi.kpi_store;
-- Expected: 5  (Y001–Y005 aktif setiap bulan)


-- ── CARD #33 — Top Store (Number / Text) ─────────────────────
-- Metabase: SQL query — top store bulan terakhir
SELECT top_store_code
FROM public_marts_kpi.kpi_store
ORDER BY month DESC LIMIT 1;
-- Expected: Y001  (Mar: Y001 revenue 290k = tertinggi)


-- ── CARD #34 — Top 3 Concentration % (Number) ───────────────
-- Metabase: kpi_store → Average of top3_concentration_pct
SELECT ROUND(AVG(top3_concentration_pct), 2) AS avg_top3_concentration
FROM public_marts_kpi.kpi_store;
-- Expected: 70.27  ((67.48 + 70.33 + 73.00) / 3)
-- Jan: Y004(290)+Y002(270)+Y003(270)=830/1230=67.48%
-- Feb: Y001(290)+Y004(200)+Y003(150)=640/910=70.33%
-- Mar: Y001(290)+Y002(240)+Y003(200)=730/1000=73.00%


-- ── CARD #35 — Distinct Products Sold (Number) ──────────────
-- Metabase: kpi_product → Max of distinct_products_sold
SELECT MAX(distinct_products_sold) AS distinct_products
FROM public_marts_kpi.kpi_product;
-- Expected: 4  (PRD001–PRD004 semua terjual setiap bulan)


-- ── CARD #36 — Product Diversity Index (Number) ─────────────
-- Metabase: kpi_product → Average of product_diversity_index
SELECT ROUND(AVG(product_diversity_index), 4) AS avg_diversity_index
FROM public_marts_kpi.kpi_product;
-- Expected: ≈ 0.7236  ((0.7472 + 0.7137 + 0.7098) / 3)
-- Mendekati 1 = distribusi revenue antar produk merata


-- ── CARD #37 — Revenue per Store (Bar Chart) ─────────────────
-- Metabase: mart_store_performance, Sum of total_revenue, Grouped by initial_store_code, Sort DESC
SELECT
    initial_store_code,
    SUM(total_revenue)          AS total_revenue_all_time,
    SUM(total_orders)           AS total_orders_all_time,
    COUNT(DISTINCT month)       AS months_active
FROM public_marts_store.mart_store_performance
GROUP BY 1 ORDER BY 2 DESC;
-- Expected (3 bulan all-time):
--   Y001 | 755 000 | 6 | 3  (Jan175k + Feb290k + Mar290k)
--   Y004 | 610 000 | 4 | 3  (Jan290k + Feb200k + Mar120k)
--   Y002 | 600 000 | 5 | 3  (Jan270k + Feb90k + Mar240k)
--   Y003 | 620 000 | 4 | 3  (Jan270k + Feb150k + Mar200k)
--   Y005 | 555 000 | 5 | 3  (Jan225k + Feb180k + Mar150k)


-- ── CARD #38 — Market Revenue Growth MoM (Line Chart) ────────
-- Metabase: kpi_store, Line: Avg of market_revenue_mom_growth_pct, X: month
SELECT
    month,
    total_market_revenue,
    market_revenue_mom_growth_pct,
    avg_revenue_per_store
FROM public_marts_kpi.kpi_store ORDER BY month;
-- Expected:
--   2026-01 | 1 230 000 |   NULL | 246 000
--   2026-02 |   910 000 | -26.02 | 182 000
--   2026-03 | 1 000 000 |   9.89 | 200 000


-- ── CARD #39 — Top Products by Quantity Sold (Horizontal Bar) ─
-- Metabase: mart_sales_by_product, Sum of total_qty_sold, Grouped by product_id, Sort DESC, Limit 10
SELECT
    product_id,
    SUM(total_qty_sold)         AS total_qty_all_time,
    SUM(total_revenue)          AS total_revenue_all_time
FROM public_marts_sales.mart_sales_by_product
GROUP BY product_id
ORDER BY total_qty_all_time DESC
LIMIT 10;
-- Expected:
--   PRD004 | 17 |   510 000  (Jan9 + Feb5 + Mar3)
--   PRD001 | 16 |   800 000  (Jan6 + Feb5 + Mar5)
--   PRD002 | 10 |   750 000  (Jan4 + Feb2 + Mar4)
--   PRD003 |  9 | 1 080 000  (Jan3 + Feb3 + Mar3)
-- Insight: PRD004 qty terbanyak tapi revenue terendah = harga murah (30k/unit)


-- ── CARD #40 — Product Revenue Share (Pie/Donut) ─────────────
-- Metabase: mart_sales_by_product, Sum of total_revenue, Grouped by product_id
SELECT
    product_id,
    SUM(total_revenue)                                              AS total_revenue,
    ROUND(SUM(total_revenue)::numeric / 3140000 * 100, 2)          AS revenue_share_pct
FROM public_marts_sales.mart_sales_by_product
GROUP BY product_id
ORDER BY total_revenue DESC;
-- Expected (total = 3 140 000):
--   PRD003 | 1 080 000 | 34.39%
--   PRD001 |   800 000 | 25.48%
--   PRD002 |   750 000 | 23.89%
--   PRD004 |   510 000 | 16.24%


-- ── CARD #41 — Product Diversity Index Trend (Line Chart) ────
-- Metabase: kpi_product, Line: Avg of product_diversity_index, X: month
SELECT
    month,
    product_diversity_index,
    distinct_products_sold,
    top_product_by_revenue,
    top_product_revenue_share_pct
FROM public_marts_kpi.kpi_product ORDER BY month;
-- Expected:
--   2026-01 | ≈0.7472 | 4 | PRD003 | 29.27%
--   2026-02 | ≈0.7137 | 4 | PRD003 | 39.56%  (PRD003 makin dominan)
--   2026-03 | ≈0.7098 | 4 | PRD003 | 36.00%


-- ── CARD #42 — Store Monthly Performance Table ───────────────
-- Metabase: mart_store_performance, pilih kolom, Sort: month DESC, revenue_rank ASC
SELECT
    initial_store_code,
    month,
    total_revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    revenue_rank,
    cumulative_revenue
FROM public_marts_store.mart_store_performance
ORDER BY month DESC, revenue_rank ASC;
-- Expected: 15 baris
-- Mar 2026 top: Y001(290k rank1), Y002(240k rank2), Y003(200k rank3)


-- ================================================================
-- BONUS A — FULL RETENTION AUDIT
-- Verifikasi manual step-by-step siapa yang retained
-- ================================================================

-- Step 1: Semua buyer per bulan dari fact_sales
SELECT
    TO_CHAR(order_date, 'YYYY-MM')                                      AS month,
    COUNT(DISTINCT customer_id)                                         AS buyer_count,
    STRING_AGG(DISTINCT customer_id, ', ' ORDER BY customer_id)        AS buyer_list
FROM public.fact_sales
GROUP BY 1 ORDER BY 1;

-- Step 2: Siapa yang retained bulan ini (beli bulan ini AND bulan sebelumnya)
WITH monthly_buyers AS (
    SELECT DISTINCT TO_CHAR(order_date, 'YYYY-MM') AS month, customer_id
    FROM public.fact_sales
)
SELECT
    curr.month,
    COUNT(DISTINCT curr.customer_id)                                        AS total_buyers,
    COUNT(DISTINCT prev.customer_id)                                        AS retained_count,
    STRING_AGG(prev.customer_id, ', ' ORDER BY prev.customer_id)            AS retained_list
FROM monthly_buyers curr
LEFT JOIN monthly_buyers prev
    ON  curr.customer_id = prev.customer_id
    AND prev.month = TO_CHAR(
            (curr.month || '-01')::date - INTERVAL '1 month', 'YYYY-MM')
WHERE prev.customer_id IS NOT NULL
GROUP BY curr.month ORDER BY curr.month;
-- Expected:
--   2026-02 | 7 | 2 | CUST002, CUST003  → 2/9 = 22.22%
--   2026-03 | 7 | 3 | CUST002, CUST003, CUST010  → 3/7 = 42.86%

-- Step 3: Cross-check dengan kpi_customer_retention (harus identik)
SELECT month, total_buyers, retained_from_prev_month, prev_month_buyers,
       retention_rate_pct, churn_rate_pct
FROM public_marts_kpi.kpi_customer_retention ORDER BY month;


-- ================================================================
-- BONUS B — STORE REVENUE BREAKDOWN (dari fact_sales langsung)
-- ================================================================

SELECT
    store_code,
    TO_CHAR(order_date, 'YYYY-MM')       AS month,
    COUNT(*)                             AS orders,
    COUNT(DISTINCT customer_id)          AS unique_customers,
    SUM(total_amount)                    AS revenue
FROM public.fact_sales
GROUP BY 1, 2 ORDER BY 2, 5 DESC;
-- Expected per bulan (5 stores per month = 15 baris total):
-- Jan: Y004(290k) Y002(270k) Y003(270k) Y005(225k) Y001(175k)
-- Feb: Y001(290k) Y004(200k) Y003(150k) Y005(180k) Y002(90k)
-- Mar: Y001(290k) Y002(240k) Y003(200k) Y005(150k) Y004(120k)
-- Cross-check dengan mart_store_performance: total revenue per bulan harus sama persis
