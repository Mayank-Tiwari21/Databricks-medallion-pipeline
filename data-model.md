# Data model

Source CSVs are the Bronze grain. Bronze adds lineage only (`_ingested_at`,
`_source_file`). Do not add `quality_check_result` until Silver.

## customers

| Column | Type | Notes |
|---|---|---|
| customer_id | INT | PK, sequential 1..10000 |
| customer_name | STRING | Synthetic |
| email | STRING | Unique, synthetic |
| country | STRING | Weighted market mix |
| signup_date | DATE | 2020-01-01 .. today |
| customer_segment | STRING | Premium / Standard / Basic |
| lifetime_value | DECIMAL(10,2) | Loosely correlated with segment; not summed from orders |

## products

| Column | Type | Notes |
|---|---|---|
| product_id | INT | PK, sequential 1..500 |
| product_name | STRING | Unique catalog name |
| category | STRING | Electronics, Clothing, Home & Kitchen, Beauty, Sports, Books, Toys, Grocery |
| price | DECIMAL(10,2) | Current list price |
| cost | DECIMAL(10,2) | Typically 40–75% of price |
| stock_quantity | INT | On-hand units |
| reorder_level | INT | Replenishment trigger |

## orders

| Column | Type | Notes |
|---|---|---|
| order_id | INT | PK, sequential 1..100000 |
| customer_id | INT | FK → customers.customer_id |
| order_date | DATE | ≥ that customer's signup_date |
| product_id | INT | FK → products.product_id |
| quantity | INT | Units ordered |
| unit_price | DECIMAL(10,2) | Price charged on the order |
| total_amount | DECIMAL(10,2) | quantity × unit_price |
| order_status | STRING | Pending / Completed / Cancelled |
| payment_date | DATE | Null when Pending; else ≥ order_date |

```
customers (1) ──< orders >── (1) products
```

## Silver flag column (customers, orders, products)

| Column | Type | Notes |
|---|---|---|
| quality_check_result | `array<string>` | Empty = passed every check. Products are always empty. |

Queryable report: `silver.quality_metrics` (`pass_pct` per table and check). Gold should use `size(quality_check_result) = 0`.

## Bronze lineage (all three tables)

| Column | Type | Notes |
|---|---|---|
| _source_file | STRING | `input_file_name()` of the CSV Spark read |
| _ingested_at | TIMESTAMP | `current_timestamp()` at write time |
