# Prompt — Silver layer

## Prompt 1 (completeness)

Write 01_quality_completeness.py: check for NULLs in customers.email, orders.customer_id, and
orders.product_id. Don't drop rows — add/update a `quality_check_result` column (array or delimited
string) that records which checks each row failed. Also compute and return a completeness % per field.

## Prompt 2 (uniqueness)

Write 02_quality_uniqueness.py: detect duplicate order_id in orders and duplicate customer_id in
customers using window functions (not a naive groupBy-count join, for performance on 100k+ rows).
Flag all rows involved in a duplicate (not just the second occurrence) in quality_check_result.
Report % unique per table.

## Prompt 3 (referential integrity)

Write 04_quality_referential_integrity.py: flag orders rows where customer_id doesn't exist in
customers, or product_id doesn't exist in products (left-anti join pattern). Report the % of orders
that are orphaned on each foreign key, and the count of distinct offending customer_id/product_id
values.

## Prompt 4 (type validation + business logic)

Write 03_quality_type_validation.py and 05_quality_business_logic.py: type-validate that
total_amount == quantity * unit_price (flag mismatches), and that signup_date / order_date aren't in
the future. Keep the same flag-don't-drop pattern as the other checks.

## Prompt 5 (create Silver tables)

Write create_silver_tables.py that runs all quality checks, merges their flags into one
quality_check_result column per row, writes Silver Delta tables (silver.customers, silver.orders,
silver.products) preserving ALL rows including flagged ones, and produces a single quality metrics
report (% passed per check, per table) as a DataFrame I can query.
