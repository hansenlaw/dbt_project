-- ============================================================
-- EXAMPLE DATA: dim_sales snapshot (timestamp strategy)
-- Shows how dbt snapshot tracks changes over 3 load dates
-- ============================================================

-- STEP 1: Create source table (if not already created)
CREATE TABLE IF NOT EXISTS public.src_sales (
    order_id        SERIAL PRIMARY KEY,
    order_date      DATE            NOT NULL,
    store_code      TEXT            NOT NULL,
    customer_id     TEXT            NOT NULL,
    product_id      TEXT            NOT NULL,
    quantity        INT             NOT NULL,
    unit_price      NUMERIC(12, 2)  NOT NULL,
    total_amount    NUMERIC(12, 2)  NOT NULL,
    partition_time  DATE            NOT NULL
);

-- ============================================================
-- BATCH 1: 2026-01-01  →  dbt snapshot --vars '{"raw_data_date":"2026-01-01"}'
-- ============================================================
INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-01-01', 'STR001', 'CUST001', 'PRD001', 2,  50000.00, 100000.00, '2026-01-01'),
    ('2026-01-01', 'STR001', 'CUST002', 'PRD002', 1,  75000.00,  75000.00, '2026-01-01'),
    ('2026-01-01', 'STR002', 'CUST003', 'PRD001', 3,  50000.00, 150000.00, '2026-01-01'),
    ('2026-01-01', 'STR002', 'CUST004', 'PRD003', 1, 120000.00, 120000.00, '2026-01-01'),
    ('2026-01-01', 'STR003', 'CUST005', 'PRD002', 2,  75000.00, 150000.00, '2026-01-01');

/*
  After running dbt snapshot on 2026-01-01, the snapshot table looks like:
  ┌──────────┬────────────┬──────────┬──────────┬──────────┬──────────┬────────────┬────────────┬──────────────────────┬─────────────────────┐
  │ order_id │ order_date │store_code│customer_id│product_id│ quantity │ unit_price │total_amount│ dbt_valid_from       │ dbt_valid_to        │
  ├──────────┼────────────┼──────────┼──────────┼──────────┼──────────┼────────────┼────────────┼──────────────────────┼─────────────────────┤
  │    1     │ 2026-01-01 │  STR001  │  CUST001 │  PRD001  │    2     │  50000.00  │ 100000.00  │ 2026-01-01 00:00:00  │ NULL  ← current     │
  │    2     │ 2026-01-01 │  STR001  │  CUST002 │  PRD002  │    1     │  75000.00  │  75000.00  │ 2026-01-01 00:00:00  │ NULL  ← current     │
  │    3     │ 2026-01-01 │  STR002  │  CUST003 │  PRD001  │    3     │  50000.00  │ 150000.00  │ 2026-01-01 00:00:00  │ NULL  ← current     │
  │    4     │ 2026-01-01 │  STR002  │  CUST004 │  PRD003  │    1     │ 120000.00  │ 120000.00  │ 2026-01-01 00:00:00  │ NULL  ← current     │
  │    5     │ 2026-01-01 │  STR003  │  CUST005 │  PRD002  │    2     │  75000.00  │ 150000.00  │ 2026-01-01 00:00:00  │ NULL  ← current     │
  └──────────┴────────────┴──────────┴──────────┴──────────┴──────────┴────────────┴────────────┴──────────────────────┴─────────────────────┘
*/

-- ============================================================
-- BATCH 2: 2026-02-01  →  dbt snapshot --vars '{"raw_data_date":"2026-02-01"}'
-- order_id 1 and 3 are updated (quantity changed)
-- order_id 6,7 are new records
-- ============================================================
INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-01-01', 'STR001', 'CUST001', 'PRD001', 5,  50000.00, 250000.00, '2026-02-01'),  -- order_id=1, qty changed 2→5
    ('2026-01-01', 'STR001', 'CUST002', 'PRD002', 1,  75000.00,  75000.00, '2026-02-01'),  -- order_id=2, unchanged
    ('2026-01-01', 'STR002', 'CUST003', 'PRD001', 4,  50000.00, 200000.00, '2026-02-01'),  -- order_id=3, qty changed 3→4
    ('2026-01-01', 'STR002', 'CUST004', 'PRD003', 1, 120000.00, 120000.00, '2026-02-01'),  -- order_id=4, unchanged
    ('2026-01-01', 'STR003', 'CUST005', 'PRD002', 2,  75000.00, 150000.00, '2026-02-01'),  -- order_id=5, unchanged
    ('2026-02-01', 'STR004', 'CUST006', 'PRD004', 3,  30000.00,  90000.00, '2026-02-01'),  -- new
    ('2026-02-01', 'STR005', 'CUST007', 'PRD001', 2,  50000.00, 100000.00, '2026-02-01');  -- new

/*
  After running dbt snapshot on 2026-02-01:
  ┌──────────┬──────────┬──────────────────────┬──────────────────────┬─────────┐
  │ order_id │ quantity │ dbt_valid_from        │ dbt_valid_to         │ status  │
  ├──────────┼──────────┼──────────────────────┼──────────────────────┼─────────┤
  │    1     │    2     │ 2026-01-01 00:00:00  │ 2026-02-01 00:00:00  │ expired │  ← old version
  │    1     │    5     │ 2026-02-01 00:00:00  │ NULL                 │ current │  ← new version
  │    2     │    1     │ 2026-01-01 00:00:00  │ NULL                 │ current │  ← unchanged
  │    3     │    3     │ 2026-01-01 00:00:00  │ 2026-02-01 00:00:00  │ expired │  ← old version
  │    3     │    4     │ 2026-02-01 00:00:00  │ NULL                 │ current │  ← new version
  │    4     │    1     │ 2026-01-01 00:00:00  │ NULL                 │ current │  ← unchanged
  │    5     │    2     │ 2026-01-01 00:00:00  │ NULL                 │ current │  ← unchanged
  │    6     │    3     │ 2026-02-01 00:00:00  │ NULL                 │ current │  ← new record
  │    7     │    2     │ 2026-02-01 00:00:00  │ NULL                 │ current │  ← new record
  └──────────┴──────────┴──────────────────────┴──────────────────────┴─────────┘
*/

-- ============================================================
-- BATCH 3: 2026-03-01  →  dbt snapshot --vars '{"raw_data_date":"2026-03-01"}'
-- order_id 2 is updated (unit_price changed)
-- ============================================================
INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-01-01', 'STR001', 'CUST001', 'PRD001', 5,   50000.00, 250000.00, '2026-03-01'),  -- order_id=1, unchanged
    ('2026-01-01', 'STR001', 'CUST002', 'PRD002', 1,   80000.00,  80000.00, '2026-03-01'),  -- order_id=2, price changed 75k→80k
    ('2026-01-01', 'STR002', 'CUST003', 'PRD001', 4,   50000.00, 200000.00, '2026-03-01'),  -- order_id=3, unchanged
    ('2026-01-01', 'STR002', 'CUST004', 'PRD003', 1,  120000.00, 120000.00, '2026-03-01'),  -- order_id=4, unchanged
    ('2026-01-01', 'STR003', 'CUST005', 'PRD002', 2,   75000.00, 150000.00, '2026-03-01'),  -- order_id=5, unchanged
    ('2026-02-01', 'STR004', 'CUST006', 'PRD004', 3,   30000.00,  90000.00, '2026-03-01'),  -- order_id=6, unchanged
    ('2026-02-01', 'STR005', 'CUST007', 'PRD001', 2,   50000.00, 100000.00, '2026-03-01');  -- order_id=7, unchanged

/*
  After running dbt snapshot on 2026-03-01 (final state):
  ┌──────────┬──────────────┬──────────────────────┬──────────────────────┬─────────┐
  │ order_id │  unit_price  │ dbt_valid_from        │ dbt_valid_to         │ status  │
  ├──────────┼──────────────┼──────────────────────┼──────────────────────┼─────────┤
  │    1     │   50000.00   │ 2026-01-01 00:00:00  │ 2026-02-01 00:00:00  │ expired │
  │    1     │   50000.00   │ 2026-02-01 00:00:00  │ NULL                 │ current │
  │    2     │   75000.00   │ 2026-01-01 00:00:00  │ 2026-03-01 00:00:00  │ expired │  ← old price
  │    2     │   80000.00   │ 2026-03-01 00:00:00  │ NULL                 │ current │  ← new price
  │    3     │   50000.00   │ 2026-01-01 00:00:00  │ 2026-02-01 00:00:00  │ expired │
  │    3     │   50000.00   │ 2026-02-01 00:00:00  │ NULL                 │ current │
  │  4,5,6,7 │     ...      │        ...           │ NULL                 │ current │
  └──────────┴──────────────┴──────────────────────┴──────────────────────┴─────────┘

  KEY CONCEPT — timestamp strategy:
  - dbt compares partition_time of each order_id between runs
  - If partition_time changes → new snapshot row created, old row gets dbt_valid_to stamped
  - dbt_valid_to = NULL means it is the CURRENT / latest version
  - dbt_valid_to = date  means it is a HISTORICAL / expired version
*/

-- ============================================================
-- QUERY: Get only current (latest) records
-- ============================================================
SELECT *
FROM public.dim_sales
WHERE dbt_valid_to IS NULL;

-- ============================================================
-- QUERY: Get full history for a specific order
-- ============================================================
SELECT
    order_id,
    quantity,
    unit_price,
    total_amount,
    partition_time,
    dbt_valid_from,
    dbt_valid_to
FROM public.dim_sales
WHERE order_id = 1
ORDER BY dbt_valid_from;
