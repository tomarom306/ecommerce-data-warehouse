-- Staging: Clean and standardize customers
{{ config(materialized='view') }}

SELECT
    customer_key,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    city,
    state,
    customer_segment,
    is_active,
    registration_date
FROM {{ source('warehouse', 'dim_customer') }}
WHERE is_current = TRUE