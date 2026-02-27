-- Customer Lifetime Value & Metrics
{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        dc.customer_id,
        dc.full_name,
        dc.customer_segment,
        dc.city,
        dc.state,
        dc.registration_date,
        COUNT(DISTINCT fo.order_id)         AS total_orders,
        ROUND(SUM(fo.total_amount)::numeric, 2)         AS lifetime_value,
        ROUND(AVG(fo.total_amount)::numeric, 2)         AS avg_order_value,
        MIN(dd.date)                        AS first_order_date,
        MAX(dd.date)                        AS last_order_date
    FROM {{ ref('stg_customers') }} dc
    JOIN {{ source('warehouse', 'fact_orders') }} fo 
        ON dc.customer_key = fo.customer_key
    JOIN {{ source('warehouse', 'dim_date') }} dd 
        ON fo.order_date_key = dd.date_key
    GROUP BY
        dc.customer_id, dc.full_name, dc.customer_segment,
        dc.city, dc.state, dc.registration_date
)

SELECT
    *,
    CASE
        WHEN total_orders >= 10 THEN 'VIP'
        WHEN total_orders >= 5  THEN 'Loyal'
        WHEN total_orders >= 2  THEN 'Repeat'
        ELSE 'One-time'
    END AS customer_tier,
    (last_order_date - first_order_date) AS customer_tenure_days
FROM customer_orders
ORDER BY lifetime_value DESC