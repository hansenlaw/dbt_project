-- ============================================================
-- Create source table: src_customer
-- ============================================================

CREATE TABLE IF NOT EXISTS public.src_customer (
    customer_id       TEXT            NOT NULL,
    full_name         TEXT            NOT NULL,
    email             TEXT            NOT NULL,
    phone             TEXT            NOT NULL,
    city              TEXT            NOT NULL,
    province          TEXT            NOT NULL,
    registration_date DATE            NOT NULL,
    tier              TEXT            NOT NULL,   -- Bronze / Silver / Gold
    partition_time    DATE            NOT NULL
);

-- ============================================================
-- Dummy data: first load 2026-01-01
-- ============================================================

INSERT INTO public.src_customer
    (customer_id, full_name, email, phone, city, province, registration_date, tier, partition_time)
VALUES
    ('CUST001', 'Andi Saputra',    'andi.saputra@email.com',    '081234567001', 'Jakarta',    'DKI Jakarta',        '2025-03-15', 'Silver', '2026-01-01'),
    ('CUST002', 'Budi Santoso',    'budi.santoso@email.com',    '081234567002', 'Surabaya',   'Jawa Timur',         '2025-04-20', 'Bronze', '2026-01-01'),
    ('CUST003', 'Citra Dewi',      'citra.dewi@email.com',      '081234567003', 'Bandung',    'Jawa Barat',         '2025-01-10', 'Gold',   '2026-01-01'),
    ('CUST004', 'Dian Pratama',    'dian.pratama@email.com',    '081234567004', 'Semarang',   'Jawa Tengah',        '2025-06-05', 'Bronze', '2026-01-01'),
    ('CUST005', 'Eko Wijaya',      'eko.wijaya@email.com',      '081234567005', 'Medan',      'Sumatera Utara',     '2025-02-28', 'Silver', '2026-01-01'),
    ('CUST006', 'Fitri Handayani', 'fitri.handayani@email.com', '081234567006', 'Yogyakarta', 'DI Yogyakarta',      '2025-07-12', 'Bronze', '2026-01-01'),
    ('CUST007', 'Gilang Ramadhan', 'gilang.ramadhan@email.com', '081234567007', 'Makassar',   'Sulawesi Selatan',   '2025-05-18', 'Silver', '2026-01-01'),
    ('CUST008', 'Hana Permata',    'hana.permata@email.com',    '081234567008', 'Palembang',  'Sumatera Selatan',   '2025-08-01', 'Bronze', '2026-01-01'),
    ('CUST009', 'Irfan Maulana',   'irfan.maulana@email.com',   '081234567009', 'Balikpapan', 'Kalimantan Timur',   '2025-03-22', 'Bronze', '2026-01-01'),
    ('CUST010', 'Joko Susilo',     'joko.susilo@email.com',     '081234567010', 'Tangerang',  'Banten',             '2025-09-14', 'Bronze', '2026-01-01'),
    ('CUST011', 'Kartika Sari',    'kartika.sari@email.com',    '081234567011', 'Bekasi',     'Jawa Barat',         '2025-10-30', 'Bronze', '2026-01-01'),
    ('CUST012', 'Lutfi Hakim',     'lutfi.hakim@email.com',     '081234567012', 'Depok',      'Jawa Barat',         '2025-11-05', 'Bronze', '2026-01-01'),
    ('CUST013', 'Maya Indah',      'maya.indah@email.com',      '081234567013', 'Bogor',      'Jawa Barat',         '2025-12-20', 'Bronze', '2026-01-01'),
    ('CUST014', 'Nanda Kurniawan', 'nanda.kurniawan@email.com', '081234567014', 'Jakarta',    'DKI Jakarta',        '2025-11-15', 'Bronze', '2026-01-01');

-- ============================================================
-- Dummy data: incremental load 2026-02-01
-- CHANGED: CUST002 Bronze→Silver, CUST004 Bronze→Silver,
--          CUST008 Bronze→Silver
-- ============================================================

INSERT INTO public.src_customer
    (customer_id, full_name, email, phone, city, province, registration_date, tier, partition_time)
VALUES
    ('CUST001', 'Andi Saputra',    'andi.saputra@email.com',    '081234567001', 'Jakarta',    'DKI Jakarta',        '2025-03-15', 'Silver', '2026-02-01'),
    ('CUST002', 'Budi Santoso',    'budi.santoso@email.com',    '081234567002', 'Surabaya',   'Jawa Timur',         '2025-04-20', 'Silver', '2026-02-01'), -- ← Bronze→Silver
    ('CUST003', 'Citra Dewi',      'citra.dewi@email.com',      '081234567003', 'Bandung',    'Jawa Barat',         '2025-01-10', 'Gold',   '2026-02-01'),
    ('CUST004', 'Dian Pratama',    'dian.pratama@email.com',    '081234567004', 'Semarang',   'Jawa Tengah',        '2025-06-05', 'Silver', '2026-02-01'), -- ← Bronze→Silver
    ('CUST005', 'Eko Wijaya',      'eko.wijaya@email.com',      '081234567005', 'Medan',      'Sumatera Utara',     '2025-02-28', 'Silver', '2026-02-01'),
    ('CUST006', 'Fitri Handayani', 'fitri.handayani@email.com', '081234567006', 'Yogyakarta', 'DI Yogyakarta',      '2025-07-12', 'Bronze', '2026-02-01'),
    ('CUST007', 'Gilang Ramadhan', 'gilang.ramadhan@email.com', '081234567007', 'Makassar',   'Sulawesi Selatan',   '2025-05-18', 'Silver', '2026-02-01'),
    ('CUST008', 'Hana Permata',    'hana.permata@email.com',    '081234567008', 'Palembang',  'Sumatera Selatan',   '2025-08-01', 'Silver', '2026-02-01'), -- ← Bronze→Silver
    ('CUST009', 'Irfan Maulana',   'irfan.maulana@email.com',   '081234567009', 'Balikpapan', 'Kalimantan Timur',   '2025-03-22', 'Bronze', '2026-02-01'),
    ('CUST010', 'Joko Susilo',     'joko.susilo@email.com',     '081234567010', 'Tangerang',  'Banten',             '2025-09-14', 'Bronze', '2026-02-01'),
    ('CUST011', 'Kartika Sari',    'kartika.sari@email.com',    '081234567011', 'Bekasi',     'Jawa Barat',         '2025-10-30', 'Bronze', '2026-02-01'),
    ('CUST012', 'Lutfi Hakim',     'lutfi.hakim@email.com',     '081234567012', 'Depok',      'Jawa Barat',         '2025-11-05', 'Bronze', '2026-02-01'),
    ('CUST013', 'Maya Indah',      'maya.indah@email.com',      '081234567013', 'Bogor',      'Jawa Barat',         '2025-12-20', 'Bronze', '2026-02-01'),
    ('CUST014', 'Nanda Kurniawan', 'nanda.kurniawan@email.com', '081234567014', 'Jakarta',    'DKI Jakarta',        '2025-11-15', 'Bronze', '2026-02-01');

-- ============================================================
-- Dummy data: incremental load 2026-03-01
-- CHANGED: CUST001 Silver→Gold, CUST007 Silver→Gold,
--          CUST010 Bronze→Silver
-- ============================================================

INSERT INTO public.src_customer
    (customer_id, full_name, email, phone, city, province, registration_date, tier, partition_time)
VALUES
    ('CUST001', 'Andi Saputra',    'andi.saputra@email.com',    '081234567001', 'Jakarta',    'DKI Jakarta',        '2025-03-15', 'Gold',   '2026-03-01'), -- ← Silver→Gold
    ('CUST002', 'Budi Santoso',    'budi.santoso@email.com',    '081234567002', 'Surabaya',   'Jawa Timur',         '2025-04-20', 'Silver', '2026-03-01'),
    ('CUST003', 'Citra Dewi',      'citra.dewi@email.com',      '081234567003', 'Bandung',    'Jawa Barat',         '2025-01-10', 'Gold',   '2026-03-01'),
    ('CUST004', 'Dian Pratama',    'dian.pratama@email.com',    '081234567004', 'Semarang',   'Jawa Tengah',        '2025-06-05', 'Silver', '2026-03-01'),
    ('CUST005', 'Eko Wijaya',      'eko.wijaya@email.com',      '081234567005', 'Medan',      'Sumatera Utara',     '2025-02-28', 'Silver', '2026-03-01'),
    ('CUST006', 'Fitri Handayani', 'fitri.handayani@email.com', '081234567006', 'Yogyakarta', 'DI Yogyakarta',      '2025-07-12', 'Bronze', '2026-03-01'),
    ('CUST007', 'Gilang Ramadhan', 'gilang.ramadhan@email.com', '081234567007', 'Makassar',   'Sulawesi Selatan',   '2025-05-18', 'Gold',   '2026-03-01'), -- ← Silver→Gold
    ('CUST008', 'Hana Permata',    'hana.permata@email.com',    '081234567008', 'Palembang',  'Sumatera Selatan',   '2025-08-01', 'Silver', '2026-03-01'),
    ('CUST009', 'Irfan Maulana',   'irfan.maulana@email.com',   '081234567009', 'Balikpapan', 'Kalimantan Timur',   '2025-03-22', 'Bronze', '2026-03-01'),
    ('CUST010', 'Joko Susilo',     'joko.susilo@email.com',     '081234567010', 'Tangerang',  'Banten',             '2025-09-14', 'Silver', '2026-03-01'), -- ← Bronze→Silver
    ('CUST011', 'Kartika Sari',    'kartika.sari@email.com',    '081234567011', 'Bekasi',     'Jawa Barat',         '2025-10-30', 'Bronze', '2026-03-01'),
    ('CUST012', 'Lutfi Hakim',     'lutfi.hakim@email.com',     '081234567012', 'Depok',      'Jawa Barat',         '2025-11-05', 'Bronze', '2026-03-01'),
    ('CUST013', 'Maya Indah',      'maya.indah@email.com',      '081234567013', 'Bogor',      'Jawa Barat',         '2025-12-20', 'Bronze', '2026-03-01'),
    ('CUST014', 'Nanda Kurniawan', 'nanda.kurniawan@email.com', '081234567014', 'Jakarta',    'DKI Jakarta',        '2025-11-15', 'Bronze', '2026-03-01');
