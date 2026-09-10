# Prompt — debugging

## Session 1 — notebook `__file__`

`ingest_all.py` (and later Silver/Gold orchestrators) raised
`NameError: __file__` when pasted into a Databricks notebook. Convert path
loading so a driver can run Bronze → Silver → Gold from disk.

## Session 2 — Free Edition landing

Databricks Free Edition has no DBFS. Change CSV source from
`/FileStore/ecommerce` to a Unity Catalog volume
(`/Volumes/workspace/default/ecommerce`), copy from repo `data/` when
present, and ignore leftover FileStore widget values.

## Outcomes

- `src/databricks_runtime.py`, `src/run_pipeline.py`
- Volume staging in `stage_landing_csvs()`
- Notes in `debugging-notes.md` and `RUN_ON_DATABRICKS.md`
