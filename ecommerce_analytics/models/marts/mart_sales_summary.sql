-- Sales Summary by Date and Category
{{ config(materialized='table') }}

SELECT
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    dp.category,
    dp.sub_category,
    COUNT(DISTINCT fo.order_id)     AS total_orders,
    SUM(foi.quantity)               AS total_units_sold,
    ROUND(SUM(foi.line_total)::numeric, 2)      AS total_revenue,
    ROUND(SUM(foi.profit)::numeric, 2)          AS total_profit,
    ROUND(AVG(fo.total_amount)::numeric, 2)     AS avg_order_value,
    ROUND(
        SUM(foi.profit) / NULLIF(SUM(foi.line_total), 0) * 100
    , 2)                            AS profit_margin_pct
FROM {{ source('warehouse', 'fact_orders') }} fo
JOIN {{ source('warehouse', 'fact_order_items') }} foi 
    ON fo.order_key = foi.order_key
JOIN {{ source('warehouse', 'dim_date') }} dd 
    ON fo.order_date_key = dd.date_key
JOIN {{ source('warehouse', 'dim_product') }} dp 
    ON foi.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY 
    dd.year, dd.month, dd.month_name, dd.quarter,
    dp.category, dp.sub_category
ORDER BY 
    dd.year, dd.month, dp.category