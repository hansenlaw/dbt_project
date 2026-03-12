-- Singular test: mart_customer_profile
-- Rule: setiap customer_id harus muncul tepat sekali (satu profil aktif)
-- Fail if any customer_id appears more than once

SELECT
    customer_id,
    COUNT(*) AS cnt
FROM {{ ref('mart_customer_profile') }}
GROUP BY 1
HAVING COUNT(*) > 1
