-- Singular test: fact_sales
-- Rule: every store_code in fact_sales must exist in working_initial_store
-- Fail if a sale references a store_code that has no SCD2 record (orphaned FK)

select
    fs.order_id,
    fs.store_code,
    fs.order_date
from {{ ref('fact_sales') }} fs
left join {{ ref('working_initial_store') }} wis
    on  fs.store_code = wis.store_code
    and fs.order_date >= wis.start_date
    and fs.order_date <  wis.end_date
where wis.store_code is null
