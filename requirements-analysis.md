# Requirements analysis

E-commerce sales medallion pipeline: source schemas, layer rules, and
dashboard needs. Runtime: Databricks Free Edition.

## Source

| File | Rows | Role |
|---|---|---|
| `data/customers.csv` | 10,000 | Customer dimension |
| `data/products.csv` | 500 | Product dimension (clean control) |
| `data/orders.csv` | 100,000 | Fact (FKs to both) |

Schemas are listed in `.cursorrules` and `data-model.md`. All data is
synthetic (Faker). No real PII.

## Layer rules (non-negotiable)

1. **Bronze** — raw ingest only. Explicit schema. Lineage columns only.
   Row count in = row count out, including nulls and duplicate PKs.
2. **Silver** — quality checks **flag** bad rows in `quality_check_result`.
   Never silently drop.
3. **Gold** — aggregations on **validated** Silver only
   (`size(quality_check_result) = 0`). No new invented source fields.
4. **Dashboard** — Gold tables only.

## Functional requirements delivered

| Need | Where |
|---|---|
| Ingest three CSVs to Delta | `src/bronze/` |
| Completeness, uniqueness, type, RI, business flags | `src/silver/01`–`05` |
| Pass-rate report | `silver.quality_metrics` |
| Sales by product | `gold.sales_by_product` |
| Revenue by customer + stated vs actual LTV | `gold.revenue_by_customer` |
| Behavioral segments | `gold.customer_segmentation` |
| Three charts (bar, distribution, pie) | `src/dashboard/` |
| Single Databricks run | `src/run_pipeline.py` |
| No DBFS on Free Edition | UC volume `/Volumes/workspace/default/ecommerce` |

## Explicitly out of scope (this pass)

- Daily / weekly trend Gold table (`03_daily_weekly_trends.sql` stub)
- Date-range dashboard filters (Gold has no `order_date`)
- Unity Catalog three-part table names in SQL
- Running Faker on the cluster

## Dashboard requirements vs implementation

| Ask | Implementation |
|---|---|
| Top products by revenue | Query 1, bar, `product_name` × `revenue_usd` |
| Customer revenue distribution | Query 2, bar of buckets (no native histogram) |
| Segment mix | Query 3, pie, **behavioral** segments not Premium/Standard/Basic |
| Optional segment filter | Documented; needs `customer_segment` on a Gold query that has it (query 2 can join) |
| Date range | Not on current Gold; called out in `DASHBOARD_GUIDE.md` |

## Non-functional

- Databricks Free Edition (Unity Catalog, no DBFS).
- Overwrite snapshots (re-runnable).
- Comments in PySpark/SQL for review, not clever one-liners.
- Seeded defects so Silver has known failure counts.

## How to run

See **`RUN_ON_DATABRICKS.md`**.
