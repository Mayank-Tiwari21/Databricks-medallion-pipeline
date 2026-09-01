# Prompt — Gold layer

## Prompt 1 (sales by product)

Write 01_sales_by_product.sql for Databricks SQL, aggregating from silver.orders joined to
silver.products, using only rows that PASSED quality checks (quality_check_result indicates clean).
Output: product_id, product_name, category, total_orders, total_revenue, avg_order_value. Exclude
Cancelled orders from revenue but explain your reasoning in a comment.

## Prompt 2 (revenue by customer)

Write 02_revenue_by_customer.sql: customer_id, customer_name, customer_segment, total_orders,
total_revenue, avg_order_value, and lifetime_value_actual (computed from real order history, so I can
compare it against the customers.lifetime_value field and sanity-check the synthetic data).

## Prompt 3 (customer segmentation)

Write 04_customer_segmentation.sql that classifies customers into High-Value / Repeat / One-Time /
Inactive based on order count and recency (propose your own thresholds and explain the reasoning —
I'll review and adjust). Output segment_type, customer_count, avg_revenue, total_revenue.

## Prompt 4 (materialize + spot checks)

Write create_gold_tables.py to run these three SQL files against Databricks and materialize them as
Gold Delta tables. Then give me 3 spot-check queries I can run manually to sanity-check the
aggregation math (e.g., does total_revenue in sales_by_product reconcile to sum(total_amount) in
silver.orders for that product).
