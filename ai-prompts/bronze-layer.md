# Prompt — Bronze layer

## Prompt 1 (ingest)

Write 01_ingest_customers.py, 02_ingest_orders.py, 03_ingest_products.py for Databricks
(PySpark). Each should: read the corresponding CSV from a Unity Catalog volume path (parameterize the path),
infer schema explicitly (define an explicit StructType rather than relying on inferSchema, since
inferSchema is unreliable on nullable/duplicate-heavy data), write to a Bronze Delta table
(bronze.customers / bronze.orders / bronze.products) with a `_ingested_at` timestamp and
`_source_file` column added, and log row counts before/after write. No cleaning, filtering, or
deduplication at this stage — Bronze is raw.

## Prompt 2 (orchestrator)

Write ingest_all.py that runs the three ingestion scripts in sequence, catches and logs failures per
table without stopping the others, and prints a final ingestion summary table (table name, row count,
status, duration).
