-- Gold: behavioral customer segmentation
--
-- Grain: one row per segment_type (four rows).
-- Source: clean silver.customers LEFT JOIN clean, non-cancelled silver.orders.
--
-- This is NOT customers.customer_segment (Premium / Standard / Basic).
-- That field is a synthetic attribute. These four labels are derived from
-- order count + recency so the dashboard can target behavior, not the
-- source-system tag.
--
-- Databricks SQL / Spark SQL. Run after src/silver/create_silver_tables.py.

CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.customer_segmentation AS
WITH
clean_customers AS (
    SELECT customer_id
    FROM silver.customers
    WHERE size(quality_check_result) = 0
),
-- Same Cancelled rule as the other Gold tables.
qualifying_orders AS (
    SELECT
        customer_id,
        order_date,
        total_amount
    FROM silver.orders
    WHERE size(quality_check_result) = 0
      AND order_status <> 'Cancelled'
),
customer_behavior AS (
    SELECT
        c.customer_id,
        COUNT(o.customer_id)                         AS order_count,
        MAX(o.order_date)                            AS last_order_date,
        COALESCE(ROUND(SUM(o.total_amount), 2), 0)   AS customer_revenue,
        DATEDIFF(CURRENT_DATE(), MAX(o.order_date))  AS days_since_last_order
    FROM clean_customers c
    LEFT JOIN qualifying_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
),
-- -------------------------------------------------------------------------
-- Thresholds — review the four-row output and adjust these two numbers.
--
-- INACTIVE_AFTER_DAYS = 365
--   Twelve months without a purchase is a common e-commerce lapse window
--   (one full seasonal cycle). Never-ordered customers (NULL last_order)
--   are Inactive too. A High-Value buyer who went quiet is also Inactive
--   here: recency wins, because the ask was count + recency, and a lapsed
--   whale is a win-back problem, not an active high-value one.
--
-- HIGH_VALUE_MIN_ORDERS = 12
--   After cancelling ~12% of orders, mean orders per customer is roughly
--   8–10 over the 2020–today window. 12 sits above that mean so High-Value
--   is the frequent tail, not "anyone who looks average." Repeat is then
--   2–11. Raise to 15 if Repeat looks too small; drop to 8 if High-Value
--   is a handful of rows.
--
-- One-Time: active (recency <= 365) and exactly one qualifying order.
-- Repeat:   active and 2 .. HIGH_VALUE_MIN_ORDERS - 1 orders.
-- -------------------------------------------------------------------------
labeled AS (
    SELECT
        customer_id,
        customer_revenue,
        CASE
            WHEN last_order_date IS NULL
              OR days_since_last_order > 365
                THEN 'Inactive'
            WHEN order_count >= 12
                THEN 'High-Value'
            WHEN order_count >= 2
                THEN 'Repeat'
            WHEN order_count = 1
                THEN 'One-Time'
            ELSE 'Inactive'
        END AS segment_type
    FROM customer_behavior
)
SELECT
    segment_type,
    COUNT(*)                              AS customer_count,
    ROUND(AVG(customer_revenue), 2)       AS avg_revenue,
    ROUND(SUM(customer_revenue), 2)       AS total_revenue
FROM labeled
GROUP BY segment_type
ORDER BY
    CASE segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat'     THEN 2
        WHEN 'One-Time'   THEN 3
        WHEN 'Inactive'   THEN 4
    END;
