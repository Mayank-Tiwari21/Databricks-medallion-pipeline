-- Databricks SQL Dashboard queries
-- Source: Gold tables only (gold.sales_by_product, gold.revenue_by_customer,
--          gold.customer_segmentation).
-- Each query is self-contained — paste one query per dashboard widget.
-- Column aliases are the chart labels (X axis / series / slice name).

-- ---------------------------------------------------------------------------
-- (1) Bar chart — Top 10 products by revenue
-- Visualization: Bar
--   X axis: product_name
--   Y axis: revenue_usd
-- Optional: color by category
-- ---------------------------------------------------------------------------
SELECT
    product_name,
    category,
    total_revenue AS revenue_usd,
    total_orders  AS order_count
FROM gold.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- (2) Histogram — customer revenue distribution
-- Visualization: Bar (Databricks SQL has no native histogram; a bar of
--   pre-bucketed counts is the usual substitute)
--   X axis: revenue_bucket   (keep sort_order so bars stay left-to-right)
--   Y axis: customer_count
--
-- Buckets cover $0 (clean customers with no qualifying orders) through
-- $2,500+. Adjust the CASE cuts after you look at the bar heights.
-- ---------------------------------------------------------------------------
SELECT
    revenue_bucket,
    sort_order,
    COUNT(*) AS customer_count
FROM (
    SELECT
        CASE
            WHEN total_revenue = 0                         THEN '$0'
            WHEN total_revenue > 0    AND total_revenue < 100   THEN '$0–$99'
            WHEN total_revenue >= 100 AND total_revenue < 250   THEN '$100–$249'
            WHEN total_revenue >= 250 AND total_revenue < 500   THEN '$250–$499'
            WHEN total_revenue >= 500 AND total_revenue < 1000  THEN '$500–$999'
            WHEN total_revenue >= 1000 AND total_revenue < 2500 THEN '$1,000–$2,499'
            ELSE '$2,500+'
        END AS revenue_bucket,
        CASE
            WHEN total_revenue = 0                         THEN 0
            WHEN total_revenue > 0    AND total_revenue < 100   THEN 1
            WHEN total_revenue >= 100 AND total_revenue < 250   THEN 2
            WHEN total_revenue >= 250 AND total_revenue < 500   THEN 3
            WHEN total_revenue >= 500 AND total_revenue < 1000  THEN 4
            WHEN total_revenue >= 1000 AND total_revenue < 2500 THEN 5
            ELSE 6
        END AS sort_order
    FROM gold.revenue_by_customer
) buckets
GROUP BY revenue_bucket, sort_order
ORDER BY sort_order;


-- ---------------------------------------------------------------------------
-- (3) Pie chart — behavioral segment mix
-- Visualization: Pie
--   Label: segment_type
--   Angle / value: customer_count   (switch to revenue_usd for a revenue pie)
--
-- segment_type is High-Value / Repeat / One-Time / Inactive
-- (from gold.customer_segmentation, NOT customers.customer_segment).
-- ---------------------------------------------------------------------------
SELECT
    segment_type,
    customer_count,
    total_revenue AS revenue_usd
FROM gold.customer_segmentation
ORDER BY
    CASE segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat'     THEN 2
        WHEN 'One-Time'   THEN 3
        WHEN 'Inactive'   THEN 4
    END;
