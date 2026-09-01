# Databricks Medallion Pipeline — E-commerce Sales

Bronze → Silver → Gold → Dashboard pipeline for an e-commerce sales use case.

**Stack:** PySpark, Delta Lake, SQL, Databricks Community Edition.

## Source data

| File | Rows | Role |
|---|---|---|
| `data/customers.csv` | 10,000 | Customers |
| `data/orders.csv` | 100,000 | Orders (FKs → customers, products) |
| `data/products.csv` | 500 | Products |

## Layers

1. **Bronze** — raw ingest only; no cleaning or transformation.
2. **Silver** — quality checks that **flag** bad rows in `quality_check_result`; never silently drop rows.
3. **Gold** — business-ready aggregations on validated Silver data.

## Current status

- [x] Project folder structure
- [x] Synthetic generators for customers, products, and orders (`src/data_generation/generate_sample_data.py`)
- [x] Bronze ingest (`src/bronze/01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`)
- [x] Silver completeness (`src/silver/01_quality_completeness.py`)
- [x] Silver uniqueness (`src/silver/02_quality_uniqueness.py`)
- [x] Silver referential integrity (`src/silver/04_quality_referential_integrity.py`)
- [x] Silver type validation (`src/silver/03_quality_type_validation.py`)
- [x] Silver business logic (`src/silver/05_quality_business_logic.py`)
- [x] Silver orchestrator + metrics (`src/silver/create_silver_tables.py`)
- [x] Gold sales by product (`src/gold/01_sales_by_product.sql`)
- [x] Gold revenue by customer (`src/gold/02_revenue_by_customer.sql`)
- [x] Gold customer segmentation (`src/gold/04_customer_segmentation.sql`)
- [x] Gold orchestrator (`src/gold/create_gold_tables.py`)
- [x] Dashboard queries (`src/dashboard/dashboard_queries.sql`)
- [x] Dashboard guide (`src/dashboard/DASHBOARD_GUIDE.md`)
- [ ] Gold daily/weekly trends
