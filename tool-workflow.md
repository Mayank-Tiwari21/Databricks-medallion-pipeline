# Tool workflow

How AI tools were used across Bronze → Silver → Gold → Dashboard.

Environment: Cursor (agent + repo rules in `.cursorrules`), local Python for
Faker generation, Databricks Free Edition for Spark/Delta.

## Pattern used on each layer

1. Read `.cursorrules` (layer rules, schema, folder map).
2. Capture the assignment prompt in `ai-prompts/<layer>.md`.
3. Implement the smallest set of files that satisfy the prompt.
4. Write notes (`DATA_GENERATION_NOTES.md`, `setup-notes.md`, etc.) with
   **assumptions** (cancelled vs pending, uniqueness flagging both dup rows,
   no DBFS, and so on).
5. Point the next layer at those notes instead of inventing columns.

## By phase

| Phase | AI role | Human / cluster role |
|---|---|---|
| Data generation | Faker script, disjoint defect injection, notes | Run locally; confirm CSV counts |
| Bronze | Explicit schemas, `ingest_all`, volume landing | Upload/Git folder; run on cluster |
| Silver | Five flag scripts + metrics orchestrator | Confirm `quality_metrics` vs seeded counts |
| Gold | Spark SQL aggregations + spot-checks | Confirm deltas vs Silver SUM are 0 |
| Dashboard | Three Gold SELECTs + viz field map | Chart in notebook on Free Edition |
| Driver | `run_pipeline.py` + notebook-safe paths | One Run all |
| Docs | This set of markdown files | Follow `RUN_ON_DATABRICKS.md` |

## Databricks-specific conversions (AI)

- `__file__` → Workspace path / widget `src_root` / `importlib` from disk.
- `/FileStore` → `/Volumes/workspace/default/ecommerce` plus copy from `data/`.
- Notebook `display()` → DataFrame `.display()` from imported modules.
- Gold `.sql` files executed statement-by-statement via `spark.sql`.

## What AI was not used for

- Inventing schema fields not in the brief.
- Dropping bad rows in Silver.
- Treating `customers.customer_segment` as the dashboard pie (behavioral
  segments are separate).
- Claiming Gold LTV matches synthetic `lifetime_value`.

## Prompt log

Exact prompts: `ai-prompts/`. Debugging sessions: `debugging-notes.md`.
Usage write-up: `final-ai-usage-summary.md`.
