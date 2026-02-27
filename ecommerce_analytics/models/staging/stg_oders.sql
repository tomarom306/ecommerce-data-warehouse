-- Staging: Clean and standardize orders
{{ config(materialized='view') }}

SELECT
    order_key,
    order_id,
    order_date_key,
    customer_key,
    payment_method_key,
    shipping_method_key,
    order_quantity,
    subtotal_amount,
    shipping_cost,
    tax_amount,
    discount_amount,
    total_amount,
    order_status,
    CASE 
        WHEN order_status = 'Completed' THEN TRUE 
        ELSE FALSE 
    END as is_completed
FROM {{ source('warehouse', 'fact_orders') }}