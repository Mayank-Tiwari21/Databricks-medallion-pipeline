# Database setup notes

Target: Databricks **Free Edition** (Spark SQL + Delta Lake + Unity Catalog).
Free Edition disables the legacy DBFS root (`/FileStore`, `/dbfs`). Landing
files go on a Unity Catalog volume. Spark then reads:

```
/Volumes/workspace/default/ecommerce/<file>.csv
```

## 1. Landing CSVs (no DBFS)

From the repo `data/` folder: `customers.csv`, `orders.csv`, `products.csv`.

**Option A — Git / Workspace folder (preferred)**

Import this project into the Databricks Workspace (Repos / Git folder) so
`data/` sits next to `src/`. `src/run_pipeline.py` / `ingest_all.py` will:

1. `CREATE VOLUME IF NOT EXISTS workspace.default.ecommerce`
2. Copy the three CSVs from `data/` onto that volume
3. Ingest with Spark from `/Volumes/workspace/default/ecommerce`

**Option B — Catalog Explorer upload**

1. Left sidebar → **Catalog** → catalog `workspace` → schema `default`.
2. **Create** → **Volume**, name `ecommerce`.
3. Open the volume → **Upload** the three CSVs.

```
/Volumes/workspace/default/ecommerce/customers.csv
/Volumes/workspace/default/ecommerce/orders.csv
/Volumes/workspace/default/ecommerce/products.csv
```

If your catalog is not named `workspace`, set widget `source_dir` to
`/Volumes/<catalog>/default/ecommerce`.

Do **not** use `/FileStore/ecommerce` — Spark cannot read DBFS root on Free
Edition.

## 2. Cluster

Attach a serverless / playground cluster that has Delta Lake (all current
Databricks runtimes do). Attach it to the notebook.

## 3. Run the pipeline (one notebook)

**Preferred:** open `src/run_pipeline.py`, attach the cluster, Run all.

That driver loads Bronze → Silver → Gold from disk (so numbered `01_*.py`
files work even though Databricks notebooks do not set `__file__`), then runs
the three dashboard `SELECT`s.

Widgets (created on the first run; set them and re-run if auto-detect fails):

| Widget | Default | Purpose |
|---|---|---|
| `source_dir` | `/Volumes/workspace/default/ecommerce` | UC Volume with the three CSVs |
| `src_root` | empty (auto) | Absolute path to `src/` |

**How `src_root` is resolved**

1. Parent of the running notebook / this `.py` file, if it contains `bronze/01_ingest_customers.py`
2. Widget `src_root` (and `src_root/src`, `.../databricks-medallion-pipeline/src`)
3. The cluster working directory

**Databricks Git folder / Repos:** clone this project, open `src/run_pipeline.py`.
Auto-detect of `src_root` usually works; landing CSVs are copied from `data/`.

If Bronze ingest fails for any table, the driver stops before Silver.

The same run then writes `silver.*` (including `silver.quality_metrics`) and
Gold tables `gold.sales_by_product`, `gold.revenue_by_customer`,
`gold.customer_segmentation`, executes the Gold spot-checks, and
`display()`s the three queries in `src/dashboard/dashboard_queries.sql`.
`03_daily_weekly_trends.sql` is not run (not written yet).

**Layer-by-layer (optional):**

```python
# widget source_dir = /Volumes/workspace/default/ecommerce
# then run src/bronze/ingest_all.py
```

Notebooks do **not** define `__file__`. Either:

1. Run `src/run_pipeline.py` (loads sibling files from disk), or
2. Run `01_ingest_customers.py`, `03_ingest_products.py`, and
   `02_ingest_orders.py` in earlier cells so `ingest_*()` already exist, or
3. Set widget `bronze_src_dir` or `src_root` to the Workspace folder that
   contains those files.

`ingest_all.py` runs customers → products → orders. If one table fails, the
error is logged and the other tables still run (the full driver then stops).
The job ends with a summary of table name, row count, status (SUCCESS/FAILED),
and duration.

**One file at a time:**

| Script | Widget `source_path` | Table |
|---|---|---|
| `01_ingest_customers.py` | `/Volumes/workspace/default/ecommerce/customers.csv` | `bronze.customers` |
| `03_ingest_products.py` | `/Volumes/workspace/default/ecommerce/products.csv` | `bronze.products` |
| `02_ingest_orders.py` | `/Volumes/workspace/default/ecommerce/orders.csv` | `bronze.orders` |

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

- Unity Catalog catalog `workspace`, schema `default`, volume `ecommerce`.
  Three-part names like `workspace.bronze.customers` are valid if you set
  that as the default catalog; this repo still uses `bronze.customers` and
  relies on the session default catalog.
- Write mode is **overwrite** so a re-run replaces the snapshot. Not append.
- `nullValue=""` treats empty CSV cells as SQL null (matches the generator).
