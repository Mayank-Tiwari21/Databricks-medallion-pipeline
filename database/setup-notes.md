# Database setup notes

Target: Databricks Community Edition (Spark SQL + Delta Lake).
Unity Catalog volumes work on paid workspaces; CE typically uses DBFS / FileStore.

## 1. Upload the CSVs

From the repo `data/` folder: `customers.csv`, `orders.csv`, `products.csv`.

**Community Edition (FileStore)**

1. Data → Create table → Upload File (or DBFS file browser).
2. Upload the three CSVs.
3. Note the DBFS path. A conventional landing folder:

```
/FileStore/ecommerce/customers.csv
/FileStore/ecommerce/orders.csv
/FileStore/ecommerce/products.csv
```

If the UI lands files under `/FileStore/tables/<name>.csv`, either move them
or point the ingest widgets at those paths.

**Workspace with Unity Catalog volumes**

```
/Volumes/<catalog>/<schema>/<volume>/customers.csv
/Volumes/<catalog>/<schema>/<volume>/orders.csv
/Volumes/<catalog>/<schema>/<volume>/products.csv
```

## 2. Cluster

Attach a cluster that has Delta Lake (all current Databricks runtimes do).
Community Edition: start the playground cluster and attach it to the notebook.

## 3. Run Bronze ingest

Import `src/bronze/` as a Databricks Repo, or paste each `01` / `02` / `03`
script into its own notebook.

**One-shot:**

```python
# widget source_dir = /FileStore/ecommerce
# then run src/bronze/ingest_all.py
```

Notebooks do **not** define `__file__`. Either:

1. Run `01_ingest_customers.py`, `03_ingest_products.py`, and
   `02_ingest_orders.py` in earlier cells so `ingest_*()` already exist, or
2. Set widget `bronze_src_dir` to the Workspace folder that contains those
   files, for example
   `/Workspace/Users/<you>/DE-C1-project/databricks-medallion-pipeline/src/bronze`.

`ingest_all.py` runs customers → products → orders. If one table fails, the
error is logged and the other tables still run. The job ends with a summary
of table name, row count, status (SUCCESS/FAILED), and duration.

**One file at a time:**

| Script | Widget `source_path` | Table |
|---|---|---|
| `01_ingest_customers.py` | `/FileStore/ecommerce/customers.csv` | `bronze.customers` |
| `03_ingest_products.py` | `/FileStore/ecommerce/products.csv` | `bronze.products` |
| `02_ingest_orders.py` | `/FileStore/ecommerce/orders.csv` | `bronze.orders` |

Each script creates database `bronze` if it does not exist, overwrites the
Delta table, and prints rows read vs rows written. Those two counts must match
(Bronze does not filter).

Expected: ~10,000 customers, 500 products, 100,000 orders — **including**
seeded nulls and duplicate PKs.

## 4. Spot-check

```sql
SELECT COUNT(*) FROM bronze.customers;   -- 10000
SELECT COUNT(*) FROM bronze.products;    -- 500
SELECT COUNT(*) FROM bronze.orders;      -- 100000

SELECT COUNT(*) FROM bronze.customers WHERE email IS NULL;           -- 50
SELECT COUNT(*) FROM bronze.orders WHERE customer_id IS NULL;        -- 100
SELECT COUNT(*) FROM bronze.orders WHERE product_id IS NULL;         -- 200
```

## Assumptions

- Hive metastore database name `bronze` (CE has no Unity Catalog catalog).
  Paid workspaces: either `USE hive_metastore` or change the target to
  `main.bronze.customers`.
- Write mode is **overwrite** so a re-run replaces the snapshot. Not append.
- `nullValue=""` treats empty CSV cells as SQL null (matches the generator).
