-- Stage the main Activity table, where the left joins and computations will happen
WITH FORMATTED_ACTIVITY AS (
    SELECT
        customer_id,
        subscription_id,
        from_date,
        to_date
    FROM {{ source('raw', 'Activity') }}
),

-- As I did in Python, for each customer, I defined their cohort date as the month of their first activity
COHORTS AS (
    SELECT
        customer_id,
        DATE_TRUNC(MIN(from_date), MONTH) AS cohort_date
    FROM FORMATTED_ACTIVITY
    GROUP BY customer_id
),

-- Further expand Activity into monthly rows by generating a row ffor every month a customer is active
EXPANDED_ACTIVITY AS (
    SELECT
        a.customer_id,
        a.subscription_id,
        DATE_TRUNC(active_date, MONTH) AS active_month
    FROM FORMATTED_ACTIVITY a,
    UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_TRUNC(a.from_date, MONTH),
            DATE_TRUNC(a.to_date, MONTH),
            INTERVAL 1 MONTH
        )
    ) AS active_date
),

-- Join the above two tables, and compute the number of months since each cohort started
RETENTION AS (
    SELECT
        c.customer_id,
        c.cohort_date,
        ea.active_month,
        DATE_DIFF(ea.active_month, c.cohort_date, MONTH) AS month_diff
    FROM COHORTS c
    JOIN EXPANDED_ACTIVITY ea
      ON c.customer_id = ea.customer_id
),

-- Compute Cohort Sizes - the number of customers that started in the cohort month
COHORT_SIZES AS (
    SELECT
        cohort_date,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM RETENTION
    WHERE month_diff = 0
    GROUP BY cohort_date
)

-- Finally, I aggregate and calculate Retention & Churn Rates
-- Just as Python, the Retention Rate is calculated excluding new customers
-- Joined the other 2 tables in order to bring in country and taxonomy
-- Cleaned/Removed the table where the active month is August 2024
-- Also cleaned/removed where taxonomy is Null
SELECT
    r.cohort_date,
    r.month_diff,
    cs.cohort_size,
    COUNT(DISTINCT r.customer_id) AS active_customers,
    SUM(CASE WHEN r.active_month = r.cohort_date THEN 1 ELSE 0 END) AS new_customers,
    CASE 
        WHEN r.month_diff = 0 THEN 100
        ELSE (
            (
                COUNT(DISTINCT r.customer_id)
                - SUM(CASE WHEN r.active_month = r.cohort_date THEN 1 ELSE 0 END)
            ) / CAST(cs.cohort_size AS FLOAT64)
        ) * 100
    END AS adjusted_retention_rate,
    CASE 
        WHEN r.month_diff = 0 THEN 0
        ELSE 100 - (
            (
                COUNT(DISTINCT r.customer_id)
                - SUM(CASE WHEN r.active_month = r.cohort_date THEN 1 ELSE 0 END)
            ) / CAST(cs.cohort_size AS FLOAT64)
        ) * 100
    END AS adjusted_churn_rate,
    cu.customer_country,
    aco.taxonomy_business_category_group
FROM RETENTION r
LEFT JOIN COHORT_SIZES cs 
  ON r.cohort_date = cs.cohort_date
LEFT JOIN {{ source('raw', 'Customers') }} cu
  ON r.customer_id = cu.customer_id
LEFT JOIN {{ source('raw', 'AcqOrders') }} aco
  ON r.customer_id = aco.customer_id
WHERE r.active_month <> DATE '2024-08-01'
  AND aco.taxonomy_business_category_group IS NOT NULL
GROUP BY
    r.cohort_date,
    r.month_diff,
    cs.cohort_size,
    cu.customer_country,
    aco.taxonomy_business_category_group
ORDER BY
    r.cohort_date,
    r.month_diff
