# Design notes

Architecture and design decisions for the Databricks medallion pipeline
(e-commerce sales). Runtime: **Databricks Free Edition** (Spark SQL, Delta
Lake, Unity Catalog). There is no DBFS.

## Pipeline shape

```
data/*.csv  ──copy──►  UC volume /Volumes/workspace/default/ecommerce
                              │
                              ▼
                         Bronze (raw Delta)
                              │
                              ▼
                    Silver (flag, do not drop)
                              │
                              ▼
                    Gold (clean rows only)
                              │
                              ▼
                    Dashboard SELECTs (Gold only)
```

One driver, `src/run_pipeline.py`, loads each layer from disk (notebooks do
not set `__file__`) and runs them in that order. Helper:
`src/databricks_runtime.py`.

## Why a Unity Catalog volume, not DBFS or Workspace files

Free Edition disables the DBFS root. Spark `read.csv` also cannot reliably
read `/Workspace/...` files. Python `Path` / `shutil` *can* read the Git
folder, so the driver copies `data/*.csv` onto
`/Volumes/workspace/default/ecommerce` and Spark reads from there.

Landing is still **raw ingest**: copy + `spark.read` with an explicit schema.
No cleaning on the way through the volume.

## Bronze

- Explicit `StructType`, all fields nullable, `mode=PERMISSIVE`, `nullValue=""`.
- No `inferSchema` — empty emails and duplicate PKs would otherwise get the
  wrong types or drop rows.
- Lineage only: `_source_file` (`input_file_name()`), `_ingested_at`.
- Write mode **overwrite** so a re-run replaces the snapshot.
- Orchestrator `ingest_all.py` runs customers → products → orders. A failure
  on one table is logged; the others still run. The **full** driver then
  stops before Silver if any Bronze table failed.

## Silver

- Flag, never drop. Column `quality_check_result` is `array<string>`.
- Empty array = passed every check. Gold filter:
  `size(quality_check_result) = 0`.
- Checks run in memory and are merged before write so each row has one array.
- `silver.quality_metrics` is a queryable pass-rate table, not a substitute
  for keeping dirty rows.
- Products are the clean control dimension (empty flag array, 500 rows).

## Gold

- Built only on Silver rows that passed every check.
- Cancelled orders are excluded from revenue, order counts, and AOV.
  Pending is kept (open pipeline, not reversed).
- `lifetime_value_actual` is SUM of those orders. Source
  `customers.lifetime_value` is a synthetic segment draw and will not match.
- Behavioral segments (High-Value / Repeat / One-Time / Inactive) are **not**
  `customers.customer_segment` (Premium / Standard / Basic).
- `03_daily_weekly_trends.sql` is not implemented; the driver skips it.

## Dashboard

- Three Gold-only SELECTs in `src/dashboard/dashboard_queries.sql`.
- Free Edition: chart the `display()` results in the notebook
  (`DASHBOARD_GUIDE.md`). SQL Warehouse dashboards are often unavailable.
- No date-range widget: current Gold tables have no `order_date`.

## Naming and catalogs

- Two-level names: `bronze.customers`, `silver.orders`, `gold.sales_by_product`.
- They resolve in the session default catalog (`workspace` on Free Edition).
- Volume path: `/Volumes/<catalog>/default/ecommerce`. Override with widget
  `source_dir` if the catalog is not `workspace`.

## What we did not add

- No invented business columns on Bronze or Silver.
- No Unity Catalog three-part table names in SQL (keeps CE/Free Edition
  two-level names).
- Data generation stays local (Faker + pandas). Databricks starts at CSV
  ingest.
