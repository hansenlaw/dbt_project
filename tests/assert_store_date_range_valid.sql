-- Singular test: working_initial_store
-- Rule: end_date must be greater than or equal to start_date for every store record
-- Fail if any row has end_date < start_date (which would indicate corrupted SCD2 logic)

select
    initial_store_code,
    store_code,
    start_date,
    end_date
from {{ ref('working_initial_store') }}
where end_date < start_date
