-- Product Performance Analysis
{{ config(materialized='table') }}

SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.sub_category,
    dp.brand,
    dp.price,
    dp.cost,
    COUNT(DISTINCT fo.order_id)         AS times_ordered,
    SUM(foi.quantity)                   AS total_units_sold,
    ROUND(SUM(foi.line_total)::numeric, 2)          AS total_revenue,
    ROUND(SUM(foi.profit)::numeric, 2)              AS total_profit,
    ROUND(AVG(foi.unit_price)::numeric, 2)          AS avg_selling_price,
    ROUND(
        SUM(foi.profit) / NULLIF(SUM(foi.line_total), 0) * 100
    , 2)                                AS profit_margin_pct,
    RANK() OVER (
        PARTITION BY dp.category 
        ORDER BY SUM(foi.line_total) DESC
    )                                   AS revenue_rank_in_category
FROM {{ source('warehouse', 'dim_product') }} dp
JOIN {{ source('warehouse', 'fact_order_items') }} foi 
    ON dp.product_key = foi.product_key
JOIN {{ source('warehouse', 'fact_orders') }} fo 
    ON foi.order_key = fo.order_key
WHERE dp.is_current = TRUE
GROUP BY
    dp.product_id, dp.product_name, dp.category,
    dp.sub_category, dp.brand, dp.price, dp.cost
ORDER BY total_revenue DESC