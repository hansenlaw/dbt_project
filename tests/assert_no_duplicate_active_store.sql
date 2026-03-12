-- Singular test: working_initial_store
-- Rule: each initial_store_code must have at most one active record (end_date = 9999-12-31)
-- Fail if any store has more than one "current" row, which would indicate SCD2 merge bug

select
    initial_store_code,
    count(*) as active_row_count
from {{ ref('working_initial_store') }}
where end_date = DATE '9999-12-31'
group by initial_store_code
having count(*) > 1
