-- Gold: sales by product
--
-- Grain: one row per product that has at least one *clean, non-cancelled* order.
-- Source: silver.orders INNER JOIN silver.products
-- Filter: quality_check_result is an empty array on BOTH sides
--         (Silver flags bad rows; Gold never uses them).
--
-- Databricks SQL / Spark SQL. Run after src/silver/create_silver_tables.py.

CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.sales_by_product AS
WITH
-- Clean = passed every Silver check. Empty array, not NULL.
clean_orders AS (
    SELECT
        product_id,
        order_status,
        total_amount
    FROM silver.orders
    WHERE size(quality_check_result) = 0
),
clean_products AS (
    SELECT
        product_id,
        product_name,
        category
    FROM silver.products
    WHERE size(quality_check_result) = 0
),
-- -------------------------------------------------------------------------
-- Cancelled orders and revenue
-- -------------------------------------------------------------------------
-- WHY we exclude Cancelled from revenue (and from total_orders / AOV):
--   A Cancelled order is a sale that did not complete. Counting its
--   total_amount would inflate revenue (the customer is not charged, or
--   the charge is reversed). total_orders and avg_order_value use the
--   same filter so AOV is revenue / orders that actually contribute,
--   not revenue / (contributing + cancelled).
--
-- Pending is kept. The brief only named Cancelled. Pending is an open
-- order whose amount may still convert; treat it as pipeline revenue.
-- If you want cash-basis revenue, add: AND order_status = 'Completed'.
-- -------------------------------------------------------------------------
qualifying_orders AS (
    SELECT
        product_id,
        total_amount
    FROM clean_orders
    WHERE order_status <> 'Cancelled'
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(*)                         AS total_orders,
    ROUND(SUM(q.total_amount), 2)    AS total_revenue,
    ROUND(AVG(q.total_amount), 2)    AS avg_order_value
FROM qualifying_orders q
INNER JOIN clean_products p
    ON q.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY
    total_revenue DESC;
