# Step-by-step: run this pipeline on Databricks Free Edition

This is the only runbook you need. Target environment: **Databricks Free Edition**
(Unity Catalog, serverless/playground compute). There is **no DBFS**
(`/FileStore` and `/dbfs` are disabled).

One notebook run executes Bronze → Silver → Gold → dashboard queries:

`src/run_pipeline.py`

---

## What you need before you start

| Item | Status in this repo |
|---|---|
| Synthetic CSVs | Already in `data/` (`customers.csv`, `orders.csv`, `products.csv`) |
| Databricks account | Free Edition workspace |
| Compute | Serverless or the playground cluster attached to the notebook |
| Git / Workspace copy of **this whole folder** | Required so `src/` and `data/` sit together |

You do **not** generate data on Databricks. Faker runs locally. The CSVs in
`data/` are the source. Refresh them only if you change the generator:

```bash
cd databricks-medallion-pipeline
python src/data_generation/generate_sample_data.py
```

---

## Step 1 — Put the project in the Workspace

1. Open [Databricks](https://databricks.com) and sign in to **Free Edition**.
2. Left sidebar → **Workspace**.
3. Import the project so this layout exists (names may vary):

```
.../databricks-medallion-pipeline/
  data/customers.csv
  data/orders.csv
  data/products.csv
  src/run_pipeline.py
  src/bronze/
  src/silver/
  src/gold/
  src/dashboard/
```

Ways that work:

- **Git folder:** Workspace → Create → Git folder → clone this repo.
- **Upload:** zip `databricks-medallion-pipeline/` and import it, or upload
  the folder under **Users / your email**.

The driver looks for `src/bronze/01_ingest_customers.py` next to
`run_pipeline.py`. If those files are missing, ingest cannot load `01_*.py`.

---

## Step 2 — Attach compute

1. Open `src/run_pipeline.py`.
2. If Databricks asks to convert it to a notebook, accept (or keep it as a
   file and **Run**).
3. Top bar → attach a cluster / **Serverless** / playground compute.
4. Wait until the cluster is **Running**.

---

## Step 3 — First run (creates widgets)

1. **Run all** (or run the last cell if the file is one cell).
2. Databricks creates widgets at the top of the notebook:

| Widget | Default | Meaning |
|---|---|---|
| `source_dir` | `/Volumes/workspace/default/ecommerce` | Unity Catalog volume Spark reads |
| `src_root` | empty | Leave empty unless auto-detect fails |
| `uc_catalog` | `workspace` | Catalog for Delta tables |
| `uc_schema` | `default` | Existing schema (do not create `bronze`) |

3. If this first run fails with “cannot find `01_ingest_customers.py`”:
   - Set `src_root` to the **Workspace path of `src/`**, for example  
     `/Workspace/Users/<you>/DE-C1-project/databricks-medallion-pipeline/src`
   - Run all again.

Do **not** set `source_dir` to `/FileStore/ecommerce`. The driver ignores that
path and uses the volume.

---

## Step 4 — How landing files get onto the volume

Free Edition Spark **cannot** read Workspace files or DBFS as CSV sources.
The driver therefore:

1. Runs `CREATE VOLUME IF NOT EXISTS workspace.default.ecommerce`
2. Copies `data/*.csv` onto `/Volumes/workspace/default/ecommerce/`
3. Runs `spark.read.csv` from that volume

**If Git/`data/` is present** you do not upload anything by hand.

**If copy fails** (volume permission, catalog name), upload by hand:

1. Sidebar → **Catalog** → catalog `workspace` → schema `default`.
2. **Create** → **Volume** named `ecommerce` (skip if it already exists).
3. Open the volume → **Upload** `customers.csv`, `orders.csv`, `products.csv`.
4. Set widget `source_dir` to `/Volumes/workspace/default/ecommerce` (or
   `/Volumes/<your_catalog>/default/ecommerce` if the catalog is not `workspace`).
5. Run all again.

---

## Step 5 — What “Run all” does (in order)

Watch the notebook output. Layers are separated by `=======` banners.

| Order | Layer | What runs | Writes |
|---|---|---|---|
| 1 | Landing | Copy CSVs to UC volume | Volume files only |
| 2 | Bronze | `ingest_all.py` → `01` customers, `03` products, `02` orders | `workspace.default.bronze_customers` / `bronze_orders` / `bronze_products` |
| 3 | Silver | Completeness, uniqueness, type, RI, business flags | `workspace.default.silver_*` + `silver_quality_metrics` |
| 4 | Gold | SQL `01`, `02`, `04` (not `03_daily_weekly_trends.sql`) | `workspace.default.gold_sales_by_product`, `gold_revenue_by_customer`, `gold_customer_segmentation` |
| 5 | Checks | Three Gold vs Silver spot-check queries | Displayed only |
| 6 | Dashboard | Three SELECTs from `dashboard_queries.sql` | Displayed only |

If **any** Bronze table status is `FAILED`, the driver **stops** before Silver.

Runtime: on Free Edition, Silver `count()`s on 100k orders can take several
minutes. That is expected.

---

## Step 6 — Confirm row counts (SQL cell or Catalog)

After a successful run, open a SQL cell (or Catalog Explorer) and run:

```sql
-- Bronze (includes seeded defects)
SELECT COUNT(*) FROM workspace.default.bronze_customers;   -- 10000
SELECT COUNT(*) FROM workspace.default.bronze_products;     -- 500
SELECT COUNT(*) FROM workspace.default.bronze_orders;       -- 100000

SELECT COUNT(*) FROM workspace.default.bronze_customers WHERE email IS NULL;        -- 50
SELECT COUNT(*) FROM workspace.default.bronze_orders WHERE customer_id IS NULL;     -- 100
SELECT COUNT(*) FROM workspace.default.bronze_orders WHERE product_id IS NULL;      -- 200
```

```sql
-- Silver keeps every row
SELECT COUNT(*) FROM workspace.default.silver_customers;    -- 10000
SELECT COUNT(*) FROM workspace.default.silver_orders;       -- 100000
SELECT COUNT(*) FROM workspace.default.silver_products;     -- 500

SELECT * FROM workspace.default.silver_quality_metrics ORDER BY table_name, check_group, check_name;
```

Expected overall pass rates (disjoint seeded defects):

- customers **99.30%** (`9930 / 10000`)
- orders **99.58%** (`99580 / 100000`)
- products **100%**

```sql
-- Gold uses only clean Silver rows
SELECT COUNT(*) FROM workspace.default.gold_sales_by_product;
SELECT COUNT(*) FROM workspace.default.gold_revenue_by_customer;      -- = clean customers
SELECT * FROM workspace.default.gold_customer_segmentation;
```

More checks: `database/queries.sql`.

---

## Step 7 — Dashboard charts

`run_pipeline.py` already **displays** the three Gold queries. To chart them
in the notebook (Free Edition often has no SQL Warehouse dashboard):

1. On each displayed result table, click the chart icon.
2. Map fields (full detail: `src/dashboard/DASHBOARD_GUIDE.md`):

| Query | Chart | X / label | Y / value |
|---|---|---|---|
| Top 10 products | Bar | `product_name` | `revenue_usd` |
| Revenue buckets | Bar | `revenue_bucket` (sort `sort_order`) | `customer_count` |
| Segment mix | Pie | `segment_type` | `customer_count` |

Paid workspaces: paste each SELECT from `src/dashboard/dashboard_queries.sql`
into its own SQL Editor query (not the whole file at once).

---

## Step 8 — Optional: run one layer at a time

Only if you are debugging a layer. Widgets are the same volume path.

| File | When |
|---|---|
| `src/bronze/ingest_all.py` | After CSVs are on the volume |
| `src/silver/create_silver_tables.py` | After Bronze tables exist |
| `src/gold/create_gold_tables.py` | After Silver tables exist |

Prefer `run_pipeline.py` for a clean end-to-end.

---

## Troubleshooting

| Symptom | What to do |
|---|---|
| `PERMISSION_DENIED: User does not have CREATE SCHEMA` | Do not create `bronze` / `silver` / `gold` databases. Tables are `workspace.default.bronze_customers` etc. Re-run the updated `run_pipeline.py`. |
| `NameError: __file__` | You pasted a layer script into a notebook. Use `run_pipeline.py` from the Git folder, or set `src_root`. |
| Cannot find `01_ingest_customers.py` | Set widget `src_root` to the `src/` Workspace path. The notebook is not sitting next to `bronze/`. |
| `/FileStore` or `dbfs:` error | Free Edition has no DBFS. Leave `source_dir` as `/Volumes/workspace/default/ecommerce`. |
| `Volume not found` / `CREATE VOLUME` failed | Create volume `ecommerce` under `workspace.default` in Catalog Explorer, upload the three CSVs, re-run. |
| Catalog is not `workspace` | Set `source_dir` to `/Volumes/<catalog>/default/ecommerce`. |
| Bronze SUCCESS but row count 0 | Wrong CSV path or empty volume. Check the `[landing] copied ...` lines. |
| Silver row count ≠ Bronze | Silver must not drop rows. Re-run Silver; inspect `quality_check_result`. |
| Gold empty | Silver flags are non-empty for every row, or tables missing. Check `size(quality_check_result) = 0`. |
| `03_daily_weekly_trends` missing | Not implemented. The driver skips it on purpose. |

---

## Assumptions (read these once)

- Tables live in **`workspace.default`** (Free Edition cannot `CREATE SCHEMA`
  on catalog `workspace`). Physical names:
  `bronze_customers`, `silver_orders`, `gold_sales_by_product`, …
- Writes are **overwrite**, not append. Re-running the driver replaces Bronze,
  Silver, and Gold snapshots.
- Cancelled orders are excluded from Gold revenue; Pending is kept.
- `customers.lifetime_value` is synthetic and will **not** equal
  `gold.revenue_by_customer.lifetime_value_actual`.
- Dashboard date filters are not wired: Gold tables have no `order_date`.
