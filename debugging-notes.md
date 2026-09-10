# Debugging notes

Issues found while building and running the pipeline, and how they were
resolved. (Part A: AI Workflow Foundation)

## 1. `NameError: name '__file__' is not defined`

**Where:** Databricks notebook (ipykernel). Pasting `ingest_all.py`,
`create_silver_tables.py`, or `create_gold_tables.py` into a cell.

**Why:** Those scripts used `Path(__file__).parent` to load sibling `01_*.py`
/ `.sql` files. Databricks notebooks do not set `__file__`. The notebook
path looks like `command-<id>`, not the repo file.

**Fix:**

- Driver `src/run_pipeline.py` finds `src/` from the notebook Workspace path,
  widget `src_root`, or `__file__` when run as a `.py`.
- It then `importlib`-loads files from disk (those modules **do** get
  `__file__`).
- Layer orchestrators fall back to `databricks_runtime`, then widgets
  (`src_root`, `bronze_src_dir`), then cwd.

**Check:** Run `src/run_pipeline.py` from the Git/Workspace folder, not a
copy-paste of a single orchestrator.

## 2. Free Edition: no DBFS (`/FileStore`, `/dbfs`)

**Where:** Bronze `spark.read.csv("/FileStore/ecommerce/...")`.

**Why:** Databricks Free Edition disables the legacy DBFS root. Spark also
cannot treat Workspace files as a CSV source the way FileStore used to work.

**Fix:** Landing path is a Unity Catalog volume:

`/Volumes/workspace/default/ecommerce`

The driver `CREATE VOLUME IF NOT EXISTS` and copies repo `data/*.csv` onto
that volume (Python can read Workspace files; Spark then reads the volume).
Old widget values starting with `/FileStore` or `dbfs:` are ignored.

**Check:** Log lines `[landing] copied customers.csv → /Volumes/...`. Bronze
row counts 10000 / 500 / 100000.

## 3. Duplicate PK injection vs uniqueness flags

**Where:** Generator injects 10 duplicate `customer_id` values (10 mutated
rows). Silver uniqueness flags **20** rows.

**Why:** Uniqueness flags **every** row in a duplicate group (donor and
victim). `COUNT(*)` over the PK window ≥ 2, so both copies are dirty.

**Fix:** Documented in `data-quality-strategy.md`. Metrics expected failed_rows
for uniqueness are 20 (customers) and 40 (orders), not 10 / 20.

## 4. Null FKs vs referential integrity

**Where:** 100 null `orders.customer_id` and 200 null `orders.product_id`.

**Why:** A left-anti join on all FKs would count nulls as orphans (NULL never
matches a parent PK). Completeness already owns those rows.

**Fix:** RI left-anti runs only where the FK is not null (and not blank).
Tokens: `ri.customer_id_orphan` (50), `ri.product_id_orphan` (30).

## 5. `display()` inside imported modules

**Where:** Dashboard helper in `databricks_runtime.py`.

**Why:** `display` is a notebook builtin on `__main__`, not on a module
loaded with `importlib`.

**Fix:** Prefer DataFrame `.display()` (Databricks Spark). Fall back to
`__main__.display`, then `show()`.

## 6. Gold `lifetime_value` vs `lifetime_value_actual`

**Where:** Spot-check on `gold.revenue_by_customer`.

**Why:** Not a bug. Generator drew `lifetime_value` from a segment range; it
did not sum orders. `lifetime_value_actual` is SUM of clean, non-cancelled
orders and **will not** match the source column.

**Check:** `delta_ltv` vs Silver SUM should be 0. `stated_ltv` vs Gold LTV
should differ.

## 7. Notebook widgets persist old defaults

**Where:** First pipeline used `/FileStore/ecommerce`. After the volume
change, `dbutils.widgets.text("source_dir", new_default)` does **not**
overwrite an existing widget value.

**Fix:** `normalize_source_dir()` rewrites FileStore/dbfs paths to the volume
default. You can also delete the widget and re-run.

## 8. `PERMISSION_DENIED: User does not have CREATE SCHEMA on Catalog 'workspace'`

**Where:** `spark.sql("CREATE DATABASE IF NOT EXISTS bronze")`.

**Why:** On Unity Catalog that statement is `CREATE SCHEMA workspace.bronze`.
Free Edition users typically cannot create schemas on catalog `workspace`.
They **can** write tables in the existing `default` schema.

**Fix:** Do not create `bronze` / `silver` / `gold` databases. Write
`workspace.default.bronze_customers`, `silver_orders`, `gold_sales_by_product`,
and so on. Widgets `uc_catalog` / `uc_schema` default to `workspace` / `default`.

## Open

- `src/gold/03_daily_weekly_trends.sql` is a stub. Not a runtime failure; the
  driver skips it.
- Pipeline was designed and syntax-checked locally. End-to-end timing and
  Free Edition volume permissions must be confirmed on a live cluster.
