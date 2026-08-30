# Databricks Medallion Pipeline — E-commerce Sales

Bronze → Silver → Gold → Dashboard pipeline for an e-commerce sales use case.

**Stack:** PySpark, Delta Lake, SQL, Databricks Community Edition.

## Source data

| File | Rows | Role |
|---|---|---|
| `data/customers.csv` | 10,000 | Customers |
| `data/orders.csv` | 100,000 | Orders (not generated yet) |
| `data/products.csv` | 500 | Products (not generated yet) |

## Layers

1. **Bronze** — raw ingest only; no cleaning or transformation.
2. **Silver** — quality checks that **flag** bad rows in `quality_check_result`; never silently drop rows.
3. **Gold** — business-ready aggregations on validated Silver data.

## Current status

- [x] Project folder structure
- [x] Synthetic `customers.csv` generator (`src/data_generation/generate_sample_data.py`)
- [ ] Orders / products generators
- [ ] Bronze ingest
- [ ] Silver quality checks
- [ ] Gold aggregations
- [ ] Dashboard
