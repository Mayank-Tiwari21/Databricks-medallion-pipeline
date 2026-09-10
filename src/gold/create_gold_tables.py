"""
Gold orchestrator — run the three aggregation SQL files and materialize
Delta tables:

    gold.sales_by_product
    gold.revenue_by_customer
    gold.customer_segmentation

Each file is executed as Spark SQL (Databricks SQL / Spark SQL). A failure
in one file is logged; the others still run.

Preferred: src/run_pipeline.py. This file no longer depends on __file__
(notebooks do not set it). Pass sql_dir, or set widget src_root.

03_daily_weekly_trends.sql is not in this pass (not written yet).
"""

from __future__ import annotations

import logging
import time
import traceback
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("gold.create_tables")


def _t(logical: str) -> str:
    try:
        from databricks_runtime import table_name

        return table_name(logical)
    except Exception:
        return "workspace.default." + logical.replace(".", "_")


def _qualify(script: str) -> str:
    try:
        from databricks_runtime import qualify_sql

        return qualify_sql(script)
    except Exception:
        return script.replace("CREATE DATABASE IF NOT EXISTS gold;", "")

GOLD_SQL_JOBS = [
    {
        "filename": "01_sales_by_product.sql",
        "target_table": "gold.sales_by_product",
    },
    {
        "filename": "02_revenue_by_customer.sql",
        "target_table": "gold.revenue_by_customer",
    },
    {
        "filename": "04_customer_segmentation.sql",
        "target_table": "gold.customer_segmentation",
    },
]

SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("sql_file", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("status", StringType(), False),
        StructField("duration_seconds", DoubleType(), False),
    ]
)

# Manual sanity checks — printed after materialize. Run in a SQL notebook.
SPOT_CHECK_QUERIES = [
    {
        "title": "1. sales_by_product.total_revenue vs SUM(silver.orders.total_amount)",
        "sql": """
-- Pick the top product in Gold and rebuild its revenue from Silver.
-- delta_revenue must be 0 (or < 0.01 for rounding).
SELECT
    g.product_id,
    g.total_orders                                          AS gold_orders,
    g.total_revenue                                         AS gold_revenue,
    COUNT(o.order_id)                                       AS silver_orders,
    ROUND(SUM(o.total_amount), 2)                           AS silver_revenue,
    ROUND(g.total_revenue - ROUND(SUM(o.total_amount), 2), 2) AS delta_revenue
FROM gold.sales_by_product g
INNER JOIN silver.orders o
    ON g.product_id = o.product_id
WHERE g.product_id = (SELECT product_id FROM gold.sales_by_product ORDER BY total_revenue DESC LIMIT 1)
  AND size(o.quality_check_result) = 0
  AND o.order_status <> 'Cancelled'
GROUP BY g.product_id, g.total_orders, g.total_revenue;
""",
    },
    {
        "title": "2. revenue_by_customer.lifetime_value_actual vs SUM of that customer's orders",
        "sql": """
-- One clean customer: Gold LTV must equal SUM of their clean, non-cancelled orders.
SELECT
    g.customer_id,
    g.total_orders,
    g.lifetime_value_actual                                 AS gold_ltv,
    COUNT(o.order_id)                                       AS silver_orders,
    ROUND(SUM(o.total_amount), 2)                           AS silver_sum,
    ROUND(g.lifetime_value_actual - ROUND(SUM(o.total_amount), 2), 2) AS delta_ltv,
    g.lifetime_value                                        AS stated_ltv
FROM gold.revenue_by_customer g
INNER JOIN silver.orders o
    ON g.customer_id = o.customer_id
WHERE g.customer_id = (
        SELECT customer_id
        FROM gold.revenue_by_customer
        WHERE total_orders > 0
        ORDER BY total_revenue DESC
        LIMIT 1
    )
  AND size(o.quality_check_result) = 0
  AND o.order_status <> 'Cancelled'
GROUP BY g.customer_id, g.total_orders, g.lifetime_value_actual, g.lifetime_value;
-- stated_ltv will NOT match gold_ltv (synthetic field vs order history).
""",
    },
    {
        "title": "3. customer_segmentation rollups vs revenue_by_customer",
        "sql": """
-- Segment customer_count must equal COUNT of clean customers.
-- Segment total_revenue must equal SUM of gold.revenue_by_customer.total_revenue
-- (same Cancelled / clean filters).
SELECT
    (SELECT SUM(customer_count) FROM gold.customer_segmentation) AS gold_segment_customers,
    (SELECT COUNT(*) FROM silver.customers WHERE size(quality_check_result) = 0) AS silver_clean_customers,
    (SELECT ROUND(SUM(total_revenue), 2) FROM gold.customer_segmentation) AS gold_segment_revenue,
    (SELECT ROUND(SUM(total_revenue), 2) FROM gold.revenue_by_customer) AS gold_customer_revenue;
-- First pair equal; second pair equal. If not, a threshold or Cancelled filter drifted.
""",
    },
]


def _get_param(name: str, default: str = "") -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


def _sql_dir(sql_dir: Path | None) -> Path:
    if sql_dir is not None:
        return Path(sql_dir)

    try:
        from databricks_runtime import layer_dir

        return layer_dir("gold")
    except Exception:
        pass

    file_val = globals().get("__file__")
    if file_val:
        here = Path(file_val).resolve().parent
        if (here / "01_sales_by_product.sql").is_file():
            return here

    widget = _get_param("src_root", "") or _get_param("gold_src_dir", "")
    candidates: list[Path] = []
    if widget.strip():
        root = Path(widget.strip())
        candidates.extend(
            [
                root,
                root / "gold",
                root / "src" / "gold",
                root / "databricks-medallion-pipeline" / "src" / "gold",
            ]
        )
    cwd = Path.cwd()
    candidates.extend(
        [
            cwd,
            cwd / "src" / "gold",
            cwd / "databricks-medallion-pipeline" / "src" / "gold",
        ]
    )
    for directory in candidates:
        try:
            directory = directory.resolve()
        except OSError:
            continue
        if (directory / "01_sales_by_product.sql").is_file():
            print(f"[gold] sql dir = {directory}")
            return directory

    raise FileNotFoundError(
        "Cannot find src/gold/01_sales_by_product.sql. Databricks notebooks "
        "have no __file__. Run src/run_pipeline.py, or set widget src_root "
        "to the src folder."
    )


def _display_or_show(df) -> None:
    if hasattr(df, "display"):
        df.display()
        return
    df.show(truncate=False)


def _sql_statements(script: str) -> list[str]:
    """Split a .sql file into Spark SQL statements.

    Full-line `--` comments are dropped first so semicolons inside comments
    cannot create empty statements. Spark.sql() runs one statement at a time.
    """
    kept: list[str] = []
    for line in script.splitlines():
        if line.lstrip().startswith("--"):
            continue
        kept.append(line)
    blob = "\n".join(kept)
    statements = [part.strip() for part in blob.split(";") if part.strip()]
    skipped = ("CREATE DATABASE", "CREATE SCHEMA")
    return [
        stmt
        for stmt in statements
        if not stmt.upper().startswith(skipped)
    ]


def _run_sql_file(spark: SparkSession, path: Path) -> None:
    script = _qualify(path.read_text(encoding="utf-8"))
    statements = _sql_statements(script)
    if not statements:
        raise ValueError(f"No SQL statements in {path.name}")
    print(f"[gold] {path.name}: {len(statements)} statement(s)")
    for i, stmt in enumerate(statements, start=1):
        preview = " ".join(stmt.split())[:80]
        print(f"[gold]   exec {i}/{len(statements)}: {preview}...")
        spark.sql(stmt)


def create_gold_tables(
    spark: SparkSession,
    sql_dir: Path | str | None = None,
) -> None:
    sql_dir = _sql_dir(Path(sql_dir) if sql_dir is not None else None)
    print(f"[gold] sql_dir = {sql_dir}")
    print("[gold] Free Edition: tables go in workspace.default (no CREATE SCHEMA)")

    results: list[tuple] = []
    for job in GOLD_SQL_JOBS:
        path = sql_dir / job["filename"]
        table = _t(job["target_table"])
        started = time.perf_counter()
        print("-" * 60)
        print(f"[gold] materializing {table} from {job['filename']}")
        try:
            _run_sql_file(spark, path)
            row_count = spark.table(table).count()
            duration = round(time.perf_counter() - started, 2)
            print(f"[gold] {table} SUCCESS  rows={row_count:,}  duration={duration}s")
            results.append((table, job["filename"], row_count, "SUCCESS", duration))
        except Exception as exc:
            duration = round(time.perf_counter() - started, 2)
            logger.exception("Gold materialize failed for %s", table)
            print(f"[gold] {table} FAILED  duration={duration}s")
            print(f"[gold] {table} error: {type(exc).__name__}: {exc}")
            print(traceback.format_exc())
            results.append((table, job["filename"], None, "FAILED", duration))

    print()
    print("=" * 72)
    print("Gold materialize summary")
    print("=" * 72)
    summary_df = spark.createDataFrame(results, schema=SUMMARY_SCHEMA)
    summary_df.show(truncate=False)

    print()
    print("=" * 72)
    print("Spot-check queries (executed against Gold + Silver)")
    print("=" * 72)
    for check in SPOT_CHECK_QUERIES:
        print()
        print(check["title"])
        statements = _sql_statements(_qualify(check["sql"]))
        if not statements:
            print(check["sql"].strip())
            continue
        try:
            _display_or_show(spark.sql(statements[0]))
        except Exception as exc:
            print(f"[gold] spot-check failed: {type(exc).__name__}: {exc}")
            print(check["sql"].strip())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    spark = SparkSession.builder.getOrCreate()
    create_gold_tables(spark)
