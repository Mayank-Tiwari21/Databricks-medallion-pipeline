# Final AI usage summary

How AI was used across data generation, Bronze, Silver, Gold, dashboard,
and debugging. (Part A: AI Workflow Foundation)

## Scope

Cursor agents implemented this repo against `.cursorrules` and the
assignment prompts stored in `ai-prompts/`. Humans run data generation
locally and the pipeline on **Databricks Free Edition**.

## By workstream

### Data generation

- **Prompts:** `ai-prompts/data-generation.md` (10k / 100k / 500 rows,
  then seeded defects).
- **Output:** `src/data_generation/generate_sample_data.py`,
  `DATA_GENERATION_NOTES.md`, `data/*.csv`.
- **AI decisions called out:** disjoint injection; uniqueness mutates 10
  IDs (Silver will flag 20 rows); products left clean; LTV not summed from
  orders.

### Bronze

- **Prompts:** `ai-prompts/bronze-layer.md`.
- **Output:** `01`/`02`/`03` ingest, `ingest_all.py`.
- **Later conversion:** landing path FileStore → UC volume; `__file__`
  removed from the happy path via `run_pipeline.py`.

### Silver

- **Prompts:** `ai-prompts/silver-layer.md`.
- **Output:** five flag scripts + `create_silver_tables.py` +
  `silver.quality_metrics`.
- **Constraint enforced by rules:** append tokens, never overwrite the
  array; never drop.

### Gold

- **Prompts:** `ai-prompts/gold-layer.md`.
- **Output:** `01_sales_by_product.sql`, `02_revenue_by_customer.sql`,
  `04_customer_segmentation.sql`, `create_gold_tables.py`.
- **Not generated:** daily/weekly trends (stub only).

### Dashboard

- **Prompts:** `ai-prompts/dashboard.md`.
- **Output:** `dashboard_queries.sql`, `DASHBOARD_GUIDE.md`.
- **Free Edition:** notebook `.display()` + chart icon; SQL Warehouse
  optional.

### Debugging / docs

- **`debugging-notes.md`:** `__file__`, DBFS, RI vs null FKs, `display()`,
  widget persistence.
- **This documentation pass:** `RUN_ON_DATABRICKS.md`, design / data model /
  DQ / requirements / workflow / reflection.

## What AI was instructed not to do

- Invent columns not in the brief.
- Drop bad rows in Silver.
- Put secrets in the repo.
- Use real customer PII in sample data.

## Result

The pipeline is complete through Gold (minus daily trends) and is meant to
be executed with **one Run all** on `src/run_pipeline.py`. Cluster execution
and volume permissions remain a human verification step.
