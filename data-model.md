# Data model

Source CSVs are the Bronze grain. Bronze adds lineage only (`_ingested_at`,
`_source_file`). Do not add `quality_check_result` until Silver.

Physical landing on Databricks Free Edition is a Unity Catalog volume
(`/Volumes/workspace/default/ecommerce`), not DBFS. Grain and columns below
are unchanged by that move.

```
customers (1) ──< orders >── (1) products
```

Catalog layout after `src/run_pipeline.py`:

| Layer | Tables |
|---|---|
| bronze | `customers`, `orders`, `products` |
| silver | `customers`, `orders`, `products`, `quality_metrics` |
| gold | `sales_by_product`, `revenue_by_customer`, `customer_segmentation` |

`gold.daily_weekly_trends` is not built yet.

---

## customers

| Column | Type | Notes |
|---|---|---|
| customer_id | INT | PK, sequential 1..10000 (10 IDs duplicated in source) |
| customer_name | STRING | Synthetic |
| email | STRING | Unique when present; 50 nulls in source |
| country | STRING | Weighted market mix |
| signup_date | DATE | 2020-01-01 .. today |
| customer_segment | STRING | Premium / Standard / Basic (source tag, not Gold segments) |
| lifetime_value | DECIMAL(10,2) | Loosely correlated with segment; **not** summed from orders |

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
| order_id | INT | PK, sequential 1..100000 (20 IDs duplicated in source) |
| customer_id | INT | FK → customers.customer_id (nulls and orphans seeded) |
| order_date | DATE | ≥ that customer's signup_date |
| product_id | INT | FK → products.product_id (nulls and orphans seeded) |
| quantity | INT | Units ordered |
| unit_price | DECIMAL(10,2) | Price charged on the order |
| total_amount | DECIMAL(10,2) | quantity × unit_price |
| order_status | STRING | Pending / Completed / Cancelled |
| payment_date | DATE | Null when Pending; else ≥ order_date |

---

## Bronze lineage (all three tables)

| Column | Type | Notes |
|---|---|---|
| _source_file | STRING | `input_file_name()` of the CSV Spark read (volume path) |
| _ingested_at | TIMESTAMP | `current_timestamp()` at write time |

Bronze row counts must equal the CSV: 10,000 / 500 / 100,000.

---

## Silver flag column (customers, orders, products)

| Column | Type | Notes |
|---|---|---|
| quality_check_result | `array<string>` | Empty = passed every check. Products are always empty. |

Queryable report: `silver.quality_metrics` (`pass_pct` per table and check).

Gold grain:

```sql
SELECT * FROM silver.orders WHERE size(quality_check_result) = 0;
```

Tokens: see `data-quality-strategy.md`.

### silver.quality_metrics

| Column | Type | Notes |
|---|---|---|
| table_name | STRING | `silver.customers` / `orders` / `products` |
| check_name | STRING | Human-readable rule |
| check_group | STRING | completeness, uniqueness, type, referential_integrity, business, overall |
| flag_token | STRING | Null for the overall “row passed all checks” rollup |
| total_rows | INT | |
| failed_rows | INT | |
| passed_rows | INT | |
| pass_pct | DOUBLE | |

---

## Gold

All Gold tables filter Silver with `size(quality_check_result) = 0`.
Cancelled orders are excluded from revenue metrics; Pending is kept.

### gold.sales_by_product

Grain: one row per product that has at least one clean, non-cancelled order.

| Column | Type | Notes |
|---|---|---|
| product_id | INT | |
| product_name | STRING | |
| category | STRING | |
| total_orders | BIGINT | Clean orders with status <> Cancelled |
| total_revenue | DECIMAL | SUM(total_amount) |
| avg_order_value | DECIMAL | AVG(total_amount) |

### gold.revenue_by_customer

Grain: one row per **clean** customer (including zero-order customers).

| Column | Type | Notes |
|---|---|---|
| customer_id | INT | Duplicate PKs are both excluded (flagged in Silver) |
| customer_name | STRING | |
| customer_segment | STRING | Source Premium / Standard / Basic |
| total_orders | BIGINT | 0 if no qualifying orders |
| total_revenue | DECIMAL | 0 if none |
| avg_order_value | DECIMAL | NULL if no qualifying orders |
| lifetime_value_actual | DECIMAL | Same SUM as total_revenue |
| lifetime_value | DECIMAL | Source synthetic LTV — will not match actual |

### gold.customer_segmentation

Grain: four rows.

| Column | Type | Notes |
|---|---|---|
| segment_type | STRING | High-Value / Repeat / One-Time / Inactive |
| customer_count | BIGINT | |
| avg_revenue | DECIMAL | Average customer spend in the segment |
| total_revenue | DECIMAL | |

Rules (clean customers, non-cancelled orders):

- **Inactive:** no qualifying order, or last order more than 365 days ago
- **High-Value:** active and ≥ 12 qualifying orders
- **Repeat:** active and 2–11 orders
- **One-Time:** active and exactly 1 order

---

## Assumptions

- Hive-style two-level names in the default Unity Catalog catalog.
- Overwrite, not append.
- No `order_date` on Gold, so dashboards cannot slice by date until daily Gold exists.
