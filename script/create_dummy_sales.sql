-- ============================================================
-- Create source table: src_sales
-- ============================================================

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
-- Batch 1: January 2026  (partition_time = '2026-01-01')
-- Stores   : Y001–Y005
-- Customers: CUST001–CUST009  (9 unique buyers)
-- Revenue  : 1,230,000
-- ============================================================

INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-01-05', 'Y001', 'CUST001', 'PRD001', 2, 50000.00,  100000.00, '2026-01-01'),
    ('2026-01-05', 'Y001', 'CUST002', 'PRD002', 1, 75000.00,   75000.00, '2026-01-01'),
    ('2026-01-08', 'Y002', 'CUST003', 'PRD001', 3, 50000.00,  150000.00, '2026-01-01'),
    ('2026-01-10', 'Y002', 'CUST004', 'PRD003', 1, 120000.00, 120000.00, '2026-01-01'),
    ('2026-01-12', 'Y003', 'CUST005', 'PRD002', 2, 75000.00,  150000.00, '2026-01-01'),
    ('2026-01-15', 'Y003', 'CUST001', 'PRD004', 4, 30000.00,  120000.00, '2026-01-01'),
    ('2026-01-18', 'Y004', 'CUST006', 'PRD001', 1, 50000.00,   50000.00, '2026-01-01'),
    ('2026-01-20', 'Y004', 'CUST007', 'PRD003', 2, 120000.00, 240000.00, '2026-01-01'),
    ('2026-01-22', 'Y005', 'CUST008', 'PRD004', 5, 30000.00,  150000.00, '2026-01-01'),
    ('2026-01-25', 'Y005', 'CUST009', 'PRD002', 1, 75000.00,   75000.00, '2026-01-01');

-- ============================================================
-- Batch 2: February 2026  (partition_time = '2026-02-01')
-- New customers: CUST010–CUST014
-- Returning from Jan: CUST002, CUST003   → retained = 2
-- Jan unique buyers  = 9
-- Retention rate     = 2 / 9 × 100 = 22.2%
-- Revenue  : 910,000
-- ============================================================

INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-02-03', 'Y001', 'CUST010', 'PRD003', 2, 120000.00, 240000.00, '2026-02-01'),
    ('2026-02-05', 'Y001', 'CUST011', 'PRD001', 1,  50000.00,  50000.00, '2026-02-01'),
    ('2026-02-08', 'Y002', 'CUST002', 'PRD004', 3,  30000.00,  90000.00, '2026-02-01'),
    ('2026-02-12', 'Y003', 'CUST012', 'PRD002', 2,  75000.00, 150000.00, '2026-02-01'),
    ('2026-02-15', 'Y004', 'CUST003', 'PRD001', 4,  50000.00, 200000.00, '2026-02-01'),
    ('2026-02-18', 'Y005', 'CUST013', 'PRD003', 1, 120000.00, 120000.00, '2026-02-01'),
    ('2026-02-22', 'Y005', 'CUST014', 'PRD004', 2,  30000.00,  60000.00, '2026-02-01');

-- ============================================================
-- Batch 3: March 2026  (partition_time = '2026-03-01')
-- Returning from Feb: CUST002, CUST003, CUST010  → retained = 3
-- Feb unique buyers  = 7
-- Retention rate     = 3 / 7 × 100 = 42.9%
-- Revenue  : 1,000,000
-- ============================================================

INSERT INTO public.src_sales
    (order_date, store_code, customer_id, product_id, quantity, unit_price, total_amount, partition_time)
VALUES
    ('2026-03-03', 'Y001', 'CUST002', 'PRD003', 2, 120000.00, 240000.00, '2026-03-01'),
    ('2026-03-05', 'Y001', 'CUST004', 'PRD001', 1,  50000.00,  50000.00, '2026-03-01'),
    ('2026-03-08', 'Y002', 'CUST003', 'PRD002', 2,  75000.00, 150000.00, '2026-03-01'),
    ('2026-03-10', 'Y002', 'CUST009', 'PRD004', 3,  30000.00,  90000.00, '2026-03-01'),
    ('2026-03-12', 'Y003', 'CUST010', 'PRD001', 4,  50000.00, 200000.00, '2026-03-01'),
    ('2026-03-15', 'Y004', 'CUST005', 'PRD003', 1, 120000.00, 120000.00, '2026-03-01'),
    ('2026-03-18', 'Y005', 'CUST008', 'PRD002', 2,  75000.00, 150000.00, '2026-03-01');
