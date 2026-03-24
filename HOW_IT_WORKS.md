# How This Project Works

A file-by-file walkthrough of the pipeline — from an empty database to a running Metabase dashboard. If you want to understand what each file does and in what order things run, this is the place to start.

---

## 1. The Big Picture

Data flows through four layers, each producing outputs that feed the next:

```
PostgreSQL (raw tables)
       ↓  script/setup.sql  — one-time data load
src_store / src_sales / src_customer
       ↓  dbt run
Intermediate layer  →  Mart layer  →  KPI layer
       ↓  consumed by
Metabase dashboard
```

The pipeline is orchestrated by dbt. You write SQL, dbt compiles it, runs it in the right order based on dependencies, and materializes the output as tables or views in PostgreSQL. GitHub Actions runs the whole thing automatically on a schedule.

---

## 2. Prerequisites

Before running anything:

- **PostgreSQL 15** — running locally, database named `demo_io`, user `postgres`
- **Python 3.10+** with `dbt-postgres` installed: `pip install dbt-postgres==1.10.0`
- **dbt profile** at `~/.dbt/profiles.yml` — see the Getting Started section in README.md

Once that's in place, verify the connection:

```bash
dbt debug
```

If it says "All checks passed", you're ready.

---

## 3. Loading the Source Data

Everything in this pipeline starts from three source tables. They don't exist until you create them.

**File: `script/setup.sql`**

Run this once against your database:

```bash
psql -U postgres -d demo_io -f script/setup.sql
```

What it does:
1. Drops and recreates `src_store`, `src_sales`, `src_customer` from scratch
2. Inserts 9 monthly batches for each table (January through September 2026)
3. Prints row counts at the end so you can confirm it worked

After running it, you should see:
- `src_store`: ~45 rows (5 stores × 9 batches, some with code changes)
- `src_sales`: 172 rows (orders spread across 9 months)
- `src_customer`: ~576 rows (64 customers × 9 batches with tier progressions)

That's all the source data. Everything else is produced by dbt.

---

## 4. File-by-File: What Each Model Does

### Intermediate layer — `models/intermediate/`

This layer sits between the raw source tables and the business marts. It handles the messy stuff.

---

**`working_initial_store.sql`** — Custom SCD Type 2

This is the most technically involved model in the project.

The problem: store codes change when branches get rebranded. `Y010` becomes `Y011` becomes `Y012`. Standard SCD2 requires a stable natural key — the source system doesn't provide one.

The solution: derive the stable key from latitude and longitude. Physical coordinates don't change when a store gets renamed. So the first store code ever seen at a (lat, lon) pair becomes the permanent `initial_store_code`. Every subsequent code change creates a new version row with `start_date` and `end_date`.

Materialization: `incremental` with `merge` strategy. On a full refresh, it uses `LEAD()` to compute end dates from the next batch's data. On incremental runs, it detects code changes and closes/opens rows via merge.

Output: one row per (initial_store_code, store_code) version.

---

**`fact_sales.sql`** — Incremental fact table

All sales transactions, deduplicated and loaded incrementally.

Materialization: `incremental` with `append` strategy. The `raw_data_date` variable controls how far back to load — it filters `WHERE partition_time <= '{{ var("raw_data_date") }}'`. On incremental runs, it skips rows already in the table using `NOT EXISTS` on `order_id`.

Post-hooks create four PostgreSQL indexes (order_date, customer_id, store_code, product_id) to keep mart queries fast.

---

**`active_store.sql`** and **`closed_store.sql`** — Views

Simple filters on `working_initial_store`. Active stores have `end_date = '9999-12-31'`, closed stores have `end_date < CURRENT_DATE`. Implemented as parameterized macro calls (`filter_store_by_status`).

---

### Snapshots — `snapshots/`

dbt Snapshots automate SCD Type 2 for dimension tables. You define which columns to track, and dbt adds `dbt_valid_from` / `dbt_valid_to` columns automatically to record when values changed.

**Snapshots must be run manually in sequence, one batch at a time:**

```bash
dbt snapshot --vars '{raw_data_date: 2026-01-01}'
dbt snapshot --vars '{raw_data_date: 2026-02-01}'
# ... repeat through 2026-09-01
```

Running them out of order or all at once produces incorrect history.

**`dim_customer.sql`** — Tracks changes to: `full_name`, `email`, `phone`, `city`, `province`, `tier`

Most important column is `tier` — as customers move Bronze → Silver → Gold, each transition gets a new row in the snapshot with timestamps. The `mart_customer_profile` model reads these rows to reconstruct the full tier journey string.

**`dim_store.sql`** — Tracks store attribute changes (area, format, channels, etc.)

**`dim_sales.sql`** — Tracks transaction corrections (not commonly used in the main pipeline)

---

### Mart layer — `models/marts/`

Business-ready tables, each aligned to a consuming team. All materialized as `table` (full rebuild on every run).

**`mart_store_performance.sql`** (schema: `marts_store`)

One row per (store, month). Pulls from `working_initial_store` + `fact_sales`. Revenue, orders, unique customers per month, plus a `RANK()` window function for monthly revenue ranking and a cumulative revenue column using `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

**`mart_store_directory.sql`** (schema: `marts_store`)

One row per store. Shows current code, active/closed status, number of rebrands, full code history string (e.g., "Y010 → Y011 → Y012"), and lifetime revenue.

**`mart_sales_summary.sql`** (schema: `marts_sales`)

Daily and monthly sales aggregates with `LAG()` for MoM revenue growth. Two grain types in one table: `day` rows and `month` rows.

**`mart_sales_by_product.sql`** (schema: `marts_sales`)

One row per (product, month). Revenue and quantity with monthly rank and a price range classification (Budget / Mid / Premium).

**`mart_sales_by_customer.sql`** (schema: `marts_sales`)

RFM segmentation. Four-stage CTE pipeline:
1. Base metrics per customer (total spend, orders, recency)
2. Score assignment (1–3 for R, F, M separately)
3. Score summing
4. Segment label mapping

**`mart_customer_profile.sql`** (schema: `marts_customer`)

Full 360° customer view. Joins purchase behavior from `mart_sales_by_customer` with identity and loyalty data from `dim_customer` snapshot. Reconstructs tier journey with `STRING_AGG(tier, ' → ' ORDER BY dbt_valid_from)`.

---

### KPI layer — `models/kpi/`

Pre-aggregated monthly metrics for the executive dashboard. Six models, all materialized as `table`.

Each of the five domain models (`kpi_revenue`, `kpi_orders`, `kpi_customer_retention`, `kpi_store`, `kpi_product`) produces one row per calendar month.

**`kpi_exec_summary.sql`** is the most important: it FULL JOINs all five on `month`, producing a single wide table — one row per month, 40+ columns. This is what Metabase reads for the executive tabs.

One pattern worth noting in `kpi_store`: getting a text value (top store code) per month requires `DISTINCT ON` in PostgreSQL rather than a GROUP BY aggregate:

```sql
SELECT DISTINCT ON (month)
    month, initial_store_code AS top_store_code, total_revenue
FROM store_monthly
ORDER BY month, total_revenue DESC
```

---

### Tests — `tests/`

Five SQL files. Each is a query that should return zero rows when data is correct. If rows come back, that's a test failure — something in the data violates the rule.

| File | Rule |
|------|------|
| `assert_store_date_range_valid.sql` | `end_date >= start_date` for all SCD2 rows |
| `assert_no_duplicate_active_store.sql` | Only one active row per store at a time |
| `assert_sales_store_exists.sql` | Every sale in `fact_sales` matches a valid store via SCD2 date-range join |
| `assert_rfm_segment_coverage.sql` | Every customer in `mart_sales_by_customer` has a non-null segment |
| `assert_no_duplicate_active_customer.sql` | Each `customer_id` appears once in `mart_customer_profile` |

---

### Macros — `macros/`

Reusable SQL logic called from models and schema.yml.

**`filter_store_by_status.sql`** — Takes a source relation and a status string ('active' or 'closed'), returns the appropriate WHERE clause. Used by `active_store.sql` and `closed_store.sql`.

**`clean_id.sql`**, **`clean_name.sql`**, **`clean_email.sql`** — Standardization helpers called in models that read from source tables.

**`tests/`** — Six custom generic test macros that can be referenced in schema.yml like built-in tests:
- `assert_positive_amount` — fails if any value ≤ 0
- `assert_positive_quantity` — same, for quantity columns
- `assert_total_amount_consistent` — checks that `total_amount ≈ qty × unit_price` within a tolerance
- `no_invalid_tier` — tier must be in {Bronze, Silver, Gold}
- `no_invalid_store_status` — status must be in {Active, Closed}
- `no_invalid_rfm_score` — RFM scores must be in {1, 2, 3}

---

### Configuration — root files

**`dbt_project.yml`** — defines the project name, model paths, schema overrides (marts get domain-specific schemas), and the `raw_data_date` variable default (`2026-09-01`).

**`packages.yml`** — no external packages used; all logic is custom.

**`models/sources.yml`** — registers the raw source tables (`src_store`, `src_sales`, `src_customer`, `src_store_detail`) so dbt can reference them with `source('retail', 'src_store')`.

---

## 5. Running the Full Pipeline (First Time)

```bash
# Step 1: Load source data (once)
psql -U postgres -d demo_io -f script/setup.sql

# Step 2: Build all intermediate, mart, and KPI models
dbt run --full-refresh

# Step 3: Run snapshots in chronological order
dbt snapshot --vars '{raw_data_date: 2026-01-01}'
dbt snapshot --vars '{raw_data_date: 2026-02-01}'
dbt snapshot --vars '{raw_data_date: 2026-03-01}'
dbt snapshot --vars '{raw_data_date: 2026-04-01}'
dbt snapshot --vars '{raw_data_date: 2026-05-01}'
dbt snapshot --vars '{raw_data_date: 2026-06-01}'
dbt snapshot --vars '{raw_data_date: 2026-07-01}'
dbt snapshot --vars '{raw_data_date: 2026-08-01}'
dbt snapshot --vars '{raw_data_date: 2026-09-01}'

# Step 4: Rebuild marts (now they have snapshot data to join against)
dbt run

# Step 5: Run all tests
dbt test
```

After this you should see 172 rows in `fact_sales`, 9 rows in `kpi_exec_summary`, 45 rows in `mart_store_performance`, and all tests passing.

---

## 6. Adding a New Monthly Batch

For each new month of data after the initial setup:

```bash
# Load new batch into source tables (depends on your source system)
# Then:

dbt run --select working_initial_store --vars '{raw_data_date: 2026-10-01}'
dbt run --select fact_sales            --vars '{raw_data_date: 2026-10-01}'
dbt snapshot                           --vars '{raw_data_date: 2026-10-01}'
dbt run --select marts.* kpi.*
dbt test
```

The incremental models detect new rows automatically via `partition_time` filtering and the `NOT EXISTS` dedup check on `order_id`.

---

## 7. CI/CD Setup

The `.github/workflows/` directory contains two workflow files.

**`dbt-ci.yml`** runs on every push to `main` and every pull request:
1. Provisions a fresh Ubuntu server
2. Installs Python + dbt
3. Generates `profiles.yml` at runtime using GitHub Secrets (never stored in the repo)
4. Runs `dbt compile` — validates syntax and model references without hitting a real database
5. Uploads logs and compiled SQL as artifacts (retained 90 days)
6. Sends Telegram alert on failure only

**`dbt-nightly.yml`** runs every night at 00:00 UTC:
- Same setup, but runs `dbt run` + `dbt test` against a full PostgreSQL container
- Sends Telegram alert regardless of outcome (daily health confirmation)

To configure alerts, add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to your GitHub repository secrets (Settings → Secrets and variables → Actions).

---

## 8. What to Check After Running

Quick verification queries (also in `script/check_table.sql`):

```sql
-- How many rows in each layer?
SELECT COUNT(*) FROM fact_sales;                          -- expect 172
SELECT COUNT(*) FROM working_initial_store;               -- expect ~15
SELECT COUNT(*) FROM public_marts_kpi.kpi_exec_summary;  -- expect 9

-- Do all customers have an RFM segment?
SELECT rfm_segment, COUNT(*) FROM public_marts_sales.mart_sales_by_customer GROUP BY 1;

-- Does every month show retention data?
SELECT month, retention_rate_pct, churn_rate_pct
FROM public_marts_kpi.kpi_customer_retention
ORDER BY month;
```

---

## 9. Dependency Map

Reading direction: an arrow means "this model reads from this source".

```
src_store ──────► working_initial_store ──► active_store
          └─────► dim_store (snapshot)      closed_store

src_sales ──────► fact_sales ──► mart_store_performance ──► kpi_store
          └─────► dim_sales       mart_store_directory
                                  mart_sales_summary   ──► kpi_orders
                                  mart_sales_by_product ─► kpi_product
                                  mart_sales_by_customer
                                                       ──► kpi_revenue
src_customer ───► dim_customer (snapshot)                  kpi_customer_retention
                       └──► mart_customer_profile
                                                       ──► kpi_exec_summary
                                                           (FULL JOIN of all 5 kpi_*)
```

---

That covers the full pipeline from an empty database to a running dashboard. For specific data quality test logic, the individual SQL files in `tests/` are each short and self-explanatory. For materialization choices and the reasoning behind them, the Materialization Reference table in README.md has the full breakdown.
