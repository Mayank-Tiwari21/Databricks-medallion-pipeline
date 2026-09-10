"""
Bronze ingest orchestrator — customers, then products, then orders.

Each table is isolated: a failure is logged and the remaining tables still
run. Bronze does not join or validate FKs; the sequence is only so a
partial success still lands the other dimensions.

Databricks:
    Set widget `source_dir` to the folder that holds the three CSVs
    (DBFS / FileStore / Volume).

    Example:
        /FileStore/ecommerce
        /Volumes/main/ecommerce/landing

    Notebooks do not define __file__. Either:
      - run 01/02/03 in earlier cells so ingest_*() already exist, or
      - set widget `bronze_src_dir` to the folder that contains those .py files
        (e.g. /Workspace/Users/<you>/.../src/bronze).
"""

from __future__ import annotations

import importlib.util
import logging
import sys
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

DEFAULT_SOURCE_DIR = "/FileStore/ecommerce"

logger = logging.getLogger("bronze.ingest_all")

# Load order: dimensions first, then the fact table. Independent of FK checks.
INGEST_JOBS = [
    {
        "filename": "01_ingest_customers.py",
        "module_name": "ingest_customers",
        "function_name": "ingest_customers",
        "csv_name": "customers.csv",
        "target_table": "bronze.customers",
    },
    {
        "filename": "03_ingest_products.py",
        "module_name": "ingest_products",
        "function_name": "ingest_products",
        "csv_name": "products.csv",
        "target_table": "bronze.products",
    },
    {
        "filename": "02_ingest_orders.py",
        "module_name": "ingest_orders",
        "function_name": "ingest_orders",
        "csv_name": "orders.csv",
        "target_table": "bronze.orders",
    },
]

SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("status", StringType(), False),
        StructField("duration_seconds", DoubleType(), False),
    ]
)


def _get_param(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _scripts_dir() -> Path:
    """Folder that contains 01_ingest_customers.py.

    Databricks notebooks (ipykernel) do not set __file__. Search, in order:
      1. this file's directory when run as a .py
      2. widget bronze_src_dir (Workspace / Repo path)
      3. the driver working directory and a few repo-shaped children
    """
    marker = "01_ingest_customers.py"
    candidates: list[Path] = []

    file_val = globals().get("__file__")
    if file_val:
        candidates.append(Path(file_val).resolve().parent)

    widget_dir = _get_param("bronze_src_dir", "")
    if widget_dir.strip():
        root = Path(widget_dir.strip())
        candidates.extend(
            [
                root,
                root / "src" / "bronze",
                root / "bronze",
                root / "databricks-medallion-pipeline" / "src" / "bronze",
            ]
        )

    cwd = Path.cwd()
    candidates.extend(
        [
            cwd,
            cwd / "src" / "bronze",
            cwd / "databricks-medallion-pipeline" / "src" / "bronze",
        ]
    )

    seen: set[Path] = set()
    for directory in candidates:
        try:
            directory = directory.resolve()
        except OSError:
            continue
        if directory in seen:
            continue
        seen.add(directory)
        if (directory / marker).is_file():
            print(f"[bronze] ingest scripts dir = {directory}")
            return directory

    raise FileNotFoundError(
        "Cannot find 01_ingest_customers.py. Databricks notebooks have no "
        "__file__. Either run 01_ingest_customers.py / 03_ingest_products.py / "
        "02_ingest_orders.py in earlier cells, or set widget bronze_src_dir to "
        "that folder (Workspace path), e.g. "
        "/Workspace/Users/<you>/DE-C1-project/databricks-medallion-pipeline/src/bronze"
    )


def _load_sibling(filename: str, module_name: str, function_name: str):
    """Load an ingest module. Names starting with a digit are not importable.

    Notebook: if ingest_customers() etc. were defined in an earlier cell, use
    those. Otherwise import the sibling .py from disk.
    """
    main = sys.modules.get("__main__")
    if main is not None and callable(getattr(main, function_name, None)):
        print(f"[bronze] using {function_name}() from the notebook kernel")
        return main

    path = _scripts_dir() / filename
    if not path.is_file():
        raise FileNotFoundError(f"Ingest script not found: {path}")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_one_table(spark: SparkSession, source_dir: str, job: dict) -> dict:
    """Run a single ingest. Never raises — failures become status=FAILED."""
    table_name = job["target_table"]
    source_path = f"{source_dir}/{job['csv_name']}"
    started = time.perf_counter()

    print("-" * 60)
    print(f"[bronze] starting {table_name}  ←  {source_path}")

    try:
        module = _load_sibling(
            job["filename"], job["module_name"], job["function_name"]
        )
        ingest_fn = getattr(module, job["function_name"])
        row_count = ingest_fn(spark, source_path, table_name)
        duration = round(time.perf_counter() - started, 2)
        print(f"[bronze] {table_name} SUCCESS  rows={row_count}  duration={duration}s")
        return {
            "table_name": table_name,
            "row_count": int(row_count) if row_count is not None else None,
            "status": "SUCCESS",
            "duration_seconds": duration,
            "error": None,
        }
    except Exception as exc:
        # Catch everything so one bad CSV / path / permission does not
        # abort products or orders. Log the traceback, then continue.
        duration = round(time.perf_counter() - started, 2)
        err_text = f"{type(exc).__name__}: {exc}"
        logger.exception("Bronze ingest failed for %s", table_name)
        print(f"[bronze] {table_name} FAILED  duration={duration}s")
        print(f"[bronze] {table_name} error: {err_text}")
        print(traceback.format_exc())
        return {
            "table_name": table_name,
            "row_count": None,
            "status": "FAILED",
            "duration_seconds": duration,
            "error": err_text,
        }


def _print_summary_table(spark: SparkSession, results: list[dict]) -> None:
    """Print the final ingestion summary (table, rows, status, duration)."""
    print()
    print("=" * 72)
    print("Bronze ingestion summary")
    print("=" * 72)

    header = f"{'table_name':<22} {'row_count':>10}  {'status':<8}  {'duration':>10}"
    print(header)
    print("-" * 72)
    for row in results:
        count_str = "-" if row["row_count"] is None else f"{row['row_count']:,}"
        duration_str = f"{row['duration_seconds']:.2f}s"
        print(
            f"{row['table_name']:<22} {count_str:>10}  {row['status']:<8}  {duration_str:>10}"
        )
    print("-" * 72)

    n_ok = sum(1 for r in results if r["status"] == "SUCCESS")
    n_fail = sum(1 for r in results if r["status"] == "FAILED")
    print(f"tables succeeded: {n_ok}   tables failed: {n_fail}")

    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        print("failures:")
        for row in failed:
            print(f"  - {row['table_name']}: {row['error']}")
    print("=" * 72)

    # Spark table so Databricks `display()` / `.show()` matches the brief.
    summary_df = spark.createDataFrame(
        [
            (
                r["table_name"],
                r["row_count"],
                r["status"],
                r["duration_seconds"],
            )
            for r in results
        ],
        schema=SUMMARY_SCHEMA,
    )
    summary_df.show(truncate=False)


def ingest_all(spark: SparkSession, source_dir: str) -> list[dict]:
    """Run all Bronze ingests. Returns one result dict per table."""
    source_dir = source_dir.rstrip("/")
    print(f"[bronze] source_dir = {source_dir}")
    print("[bronze] raw ingest only — no cleaning, filtering, or dedupe")
    print("[bronze] per-table failures are logged; remaining tables still run")

    results: list[dict] = []
    for job in INGEST_JOBS:
        results.append(_run_one_table(spark, source_dir, job))

    _print_summary_table(spark, results)
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    spark = SparkSession.builder.getOrCreate()
    source_dir = _get_param("source_dir", DEFAULT_SOURCE_DIR)
    ingest_all(spark, source_dir)
