-- Singular test: mart_sales_by_customer
-- Rule: every customer must be assigned a segment — no NULLs allowed
-- Fail if any customer has a NULL or unrecognised segment value

select
    customer_id,
    customer_segment
from {{ ref('mart_sales_by_customer') }}
where customer_segment is null
   or customer_segment not in (
       'Champions',
       'Loyal',
       'Promising',
       'At Risk',
       'Need Attention'
   )
