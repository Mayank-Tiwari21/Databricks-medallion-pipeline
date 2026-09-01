-- Gold: revenue by customer
--
-- Grain: one row per *clean* customer (including those with no qualifying
--         orders — revenue/AOV/LTV_actual are 0).
-- Source: silver.customers LEFT JOIN aggregated silver.orders
-- Filter: size(quality_check_result) = 0 on both tables.
--
-- lifetime_value_actual is summed from order history so it can be compared
-- to customers.lifetime_value. Those two columns will NOT match: the
-- generator drew lifetime_value from a segment range; it did not sum orders.
-- That gap is the synthetic-data sanity check this table exists for.
--
-- Databricks SQL / Spark SQL. Run after src/silver/create_silver_tables.py.

CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.revenue_by_customer AS
WITH
clean_customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_segment,
        lifetime_value
    FROM silver.customers
    WHERE size(quality_check_result) = 0
),
clean_orders AS (
    SELECT
        customer_id,
        order_status,
        total_amount
    FROM silver.orders
    WHERE size(quality_check_result) = 0
),
-- Same Cancelled rule as gold.sales_by_product: a cancelled sale did not
-- complete, so it must not inflate revenue, order count, AOV, or LTV.
-- Pending is kept (open pipeline). For cash-basis, restrict to Completed.
qualifying_orders AS (
    SELECT
        customer_id,
        total_amount
    FROM clean_orders
    WHERE order_status <> 'Cancelled'
),
order_rollup AS (
    SELECT
        customer_id,
        COUNT(*)                      AS total_orders,
        ROUND(SUM(total_amount), 2)   AS total_revenue,
        ROUND(AVG(total_amount), 2)   AS avg_order_value,
        -- Identical filter to total_revenue: "actual LTV" = recognized spend.
        ROUND(SUM(total_amount), 2)   AS lifetime_value_actual
    FROM qualifying_orders
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    COALESCE(r.total_orders, 0)            AS total_orders,
    COALESCE(r.total_revenue, 0)           AS total_revenue,
    r.avg_order_value,                     -- NULL when the customer has no qualifying orders
    COALESCE(r.lifetime_value_actual, 0)   AS lifetime_value_actual,
    -- Source field kept so you can compare in one table:
    --   SELECT customer_segment,
    --          AVG(lifetime_value)        AS ltv_stated,
    --          AVG(lifetime_value_actual) AS ltv_from_orders
    --   FROM gold.revenue_by_customer
    --   GROUP BY customer_segment;
    c.lifetime_value                       AS lifetime_value
FROM clean_customers c
LEFT JOIN order_rollup r
    ON c.customer_id = r.customer_id
ORDER BY
    total_revenue DESC;

-- Duplicate customer_id rows are uniqueness-flagged in Silver, so BOTH copies
-- are excluded here (Gold does not pick a survivor). Their clean orders will
-- not appear in this table because of the LEFT JOIN from clean customers.
