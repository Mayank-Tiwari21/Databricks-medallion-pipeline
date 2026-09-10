# Candidate info

Fill this in for the AI-capability exercise submission. Do not commit
secrets, tokens, or workspace passwords.

| Field | Value |
|---|---|
| Name | |
| Email | |
| Databricks workspace | Free Edition (no DBFS) |
| Project folder | `databricks-medallion-pipeline/` |
| How to run | `RUN_ON_DATABRICKS.md` |
| Driver notebook / file | `src/run_pipeline.py` |

## What reviewers should run

1. Follow `RUN_ON_DATABRICKS.md` (Git folder + Run all on the driver).
2. Check Bronze counts 10000 / 500 / 100000.
3. Check `silver.quality_metrics` against `data-quality-strategy.md`.
4. Check Gold spot-checks in `database/queries.sql`.
5. Chart the three dashboard queries (field map in
   `src/dashboard/DASHBOARD_GUIDE.md`).

## Known gaps to mention

- `src/gold/03_daily_weekly_trends.sql` not implemented.
- Dashboard has no date-range filter (Gold has no `order_date`).
