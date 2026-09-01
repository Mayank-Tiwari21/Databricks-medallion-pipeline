
--  Product revenue vs Silver SUM(total_amount)
SELECT
    g.product_id,
    g.total_orders                                          AS gold_orders,
    g.total_revenue                                         AS gold_revenue,
    COUNT(o.order_id)                                       AS silver_orders,
    ROUND(SUM(o.total_amount), 2)                           AS silver_revenue,
    ROUND(g.total_revenue - ROUND(SUM(o.total_amount), 2), 2) AS delta_revenue
FROM gold.sales_by_product g
INNER JOIN silver.orders o
    ON g.product_id = o.product_id
WHERE g.product_id = (SELECT product_id FROM gold.sales_by_product ORDER BY total_revenue DESC LIMIT 1)
  AND size(o.quality_check_result) = 0
  AND o.order_status <> 'Cancelled'
GROUP BY g.product_id, g.total_orders, g.total_revenue;

-- Customer lifetime_value_actual vs that customer’s order sum
SELECT
    g.customer_id,
    g.total_orders,
    g.lifetime_value_actual                                 AS gold_ltv,
    COUNT(o.order_id)                                       AS silver_orders,
    ROUND(SUM(o.total_amount), 2)                           AS silver_sum,
    ROUND(g.lifetime_value_actual - ROUND(SUM(o.total_amount), 2), 2) AS delta_ltv,
    g.lifetime_value                                        AS stated_ltv
FROM gold.revenue_by_customer g
INNER JOIN silver.orders o
    ON g.customer_id = o.customer_id
WHERE g.customer_id = (
        SELECT customer_id FROM gold.revenue_by_customer
        WHERE total_orders > 0 ORDER BY total_revenue DESC LIMIT 1
    )
  AND size(o.quality_check_result) = 0
  AND o.order_status <> 'Cancelled'
GROUP BY g.customer_id, g.total_orders, g.lifetime_value_actual, g.lifetime_value;

-- Segmentation totals vs clean customers and revenue_by_customer
SELECT
    (SELECT SUM(customer_count) FROM gold.customer_segmentation) AS gold_segment_customers,
    (SELECT COUNT(*) FROM silver.customers WHERE size(quality_check_result) = 0) AS silver_clean_customers,
    (SELECT ROUND(SUM(total_revenue), 2) FROM gold.customer_segmentation) AS gold_segment_revenue,
    (SELECT ROUND(SUM(total_revenue), 2) FROM gold.revenue_by_customer) AS gold_customer_revenue;

-- spark.sql() call — statements are split on ;. The script needs __file__ (Repo .py). Not executed here.