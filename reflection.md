# Reflection

What worked, what did not, and what I would do differently.
(Part A: AI Workflow Foundation)

## What worked

- **Layer rules in `.cursorrules`** stopped Bronze from cleaning and Silver
  from dropping rows. The AI followed those constraints when generating
  ingest and quality code.
- **Seeded, disjoint defects** made Silver testable. Completeness vs RI vs
  uniqueness counts can be checked independently.
- **A single Databricks driver** (`run_pipeline.py`) was the right response
  to “run the main folder and everything else runs.” Numbered `01_*.py`
  files cannot be imported as packages; loading by path is the Databricks
  way to keep them as reviewable scripts.
- **Documenting assumptions** (Cancelled excluded, Pending kept, uniqueness
  flags both copies, synthetic LTV ≠ order sum) avoided silent wrong Gold
  numbers.

## What did not work the first time

- Treating Databricks like local Python: `__file__` and `/FileStore` both
  failed. Free Edition is Unity Catalog volumes, not Community Edition
  FileStore.
- Pasting orchestrators into empty notebooks. The code is correct as **repo
  files**; the notebook is only the entry point.
- Gold daily/weekly trends were left as a stub. Dashboard date filters were
  specified in the brief but cannot be honest on Gold tables that have no
  date. Better to omit the filter than to query Silver from the dashboard.

## What I would do differently

- Start the Databricks runbook on **day one** (volume path, no `__file__`)
  instead of converting after the first cluster error.
- Add a tiny “expected metrics” assertion notebook so pass_pct is checked
  automatically after Silver, not only by eye.
- Implement `03_daily_weekly_trends.sql` before promising a date slider.
- Keep `ai-prompts/*.md` updated in the same commit as the code they
  produced, so the review trail is complete.

## AI collaboration notes

Prompts that named **exact counts**, **flag-not-drop**, and **do not invent
columns** produced usable code. Open-ended “make a dashboard” without “Gold
tables only” would have joined Bronze. Short follow-ups (“Free Edition has
no DBFS”) were more effective than rewriting the whole ingest layer.
