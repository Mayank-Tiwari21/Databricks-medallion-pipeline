"""
Databricks pipeline driver.

Run this notebook / file once. It executes:

    Bronze ingest  →  Silver quality flags  →  Gold aggregations  →  dashboard queries

Widgets (optional):
    src_root     this src/ folder    auto-detected from the notebook path
    uc_catalog   workspace
    uc_schema    default

Free Edition: Python reads Workspace data/*.csv (Spark file sources use
dbfs: and fail). Bronze tables are written in workspace.default.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession


def _get_param(name: str, default: str = "") -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


def _notebook_folder_candidates() -> list[Path]:
    try:
        ctx = (
            dbutils.notebook.entry_point.getDbutils()  # noqa: F821
            .notebook()
            .getContext()
        )
        raw = ctx.notebookPath().get()
    except Exception:
        return []
    if not raw:
        return []
    rel = str(raw).lstrip("/")
    folders: list[Path] = []
    for notebook in (Path("/Workspace") / rel, Path("/") / rel):
        parent = notebook.parent
        folders.extend([parent, parent / "src", parent.parent / "src"])
    return folders


def _find_src_root() -> Path:
    """Locate src/ without __file__ (notebook-safe)."""
    marker = Path("bronze") / "01_ingest_customers.py"
    candidates: list[Path] = []

    candidates.extend(_notebook_folder_candidates())

    widget = _get_param("src_root", "")
    if widget.strip():
        root = Path(widget.strip())
        candidates.extend(
            [
                root,
                root / "src",
                root / "databricks-medallion-pipeline" / "src",
            ]
        )

    file_val = globals().get("__file__")
    if file_val:
        candidates.append(Path(file_val).resolve().parent)

    cwd = Path.cwd()
    candidates.extend(
        [
            cwd,
            cwd / "src",
            cwd / "databricks-medallion-pipeline" / "src",
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
            print(f"[pipeline] src_root = {directory}")
            return directory

    raise FileNotFoundError(
        "Cannot find src/bronze/01_ingest_customers.py. "
        "Set widget src_root to the src folder, for example "
        "/Workspace/Users/<you>/DE-C1-project/databricks-medallion-pipeline/src"
    )


def _load_runtime(src: Path):
    path = src / "databricks_runtime.py"
    spec = importlib.util.spec_from_file_location("databricks_runtime", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["databricks_runtime"] = module
    return module


def _split_dashboard_selects(sql_text: str) -> list[tuple[str, str]]:
    """Split dashboard_queries.sql into (title, SELECT) pairs."""
    titles = [
        "Dashboard 1 — Top 10 products by revenue (bar)",
        "Dashboard 2 — Customer revenue distribution (histogram bar)",
        "Dashboard 3 — Customer segment mix (pie)",
    ]
    chunks = []
    buf: list[str] = []
    for line in sql_text.splitlines():
        if line.startswith("-- (") and buf:
            chunks.append("\n".join(buf))
            buf = [line]
        else:
            buf.append(line)
    if buf:
        chunks.append("\n".join(buf))

    selects: list[tuple[str, str]] = []
    idx = 0
    for chunk in chunks:
        start = chunk.upper().find("\nSELECT")
        if start < 0:
            start = chunk.upper().find("SELECT")
        if start < 0:
            continue
        stmt = chunk[start:].strip().rstrip(";")
        title = titles[idx] if idx < len(titles) else f"Dashboard query {idx + 1}"
        selects.append((title, stmt))
        idx += 1
    return selects


def _run_layer(name: str, fn) -> None:
    started = time.perf_counter()
    print()
    print("=" * 72)
    print(f"[pipeline] LAYER {name}")
    print("=" * 72)
    fn()
    print(f"[pipeline] LAYER {name} done in {time.perf_counter() - started:.1f}s")


def run_pipeline(spark: SparkSession | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    spark = spark or SparkSession.builder.getOrCreate()
    src = _find_src_root()
    rt = _load_runtime(src)
    _get_param("uc_catalog", rt.DEFAULT_UC_CATALOG)
    _get_param("uc_schema", rt.DEFAULT_UC_SCHEMA)
    source_dir = _get_param("source_dir", rt.DEFAULT_SOURCE_DIR).rstrip("/")
    source_dir = rt.normalize_source_dir(source_dir)

    bronze = rt.load_py("bronze/ingest_all.py", "bronze_ingest_all")
    silver = rt.load_py("silver/create_silver_tables.py", "silver_create_tables")
    gold = rt.load_py("gold/create_gold_tables.py", "gold_create_tables")

    print("[pipeline] Bronze → Silver → Gold → dashboard")
    print(f"[pipeline] CSV source_dir = {source_dir}")
    print(
        f"[pipeline] tables = {rt.uc_catalog()}.{rt.uc_schema()}."
        "bronze_* / silver_* / gold_*  (no CREATE SCHEMA)"
    )

    bronze_results: list[dict] = []

    def _bronze() -> None:
        nonlocal bronze_results
        bronze_results = bronze.ingest_all(spark, source_dir)

    _run_layer("BRONZE", _bronze)
    failed = [r for r in bronze_results if r.get("status") == "FAILED"]
    if failed:
        names = ", ".join(r["table_name"] for r in failed)
        raise RuntimeError(f"Bronze ingest failed for {names} — stop before Silver.")

    _run_layer("SILVER", lambda: silver.create_silver_tables(spark))
    _run_layer(
        "GOLD",
        lambda: gold.create_gold_tables(spark, sql_dir=rt.layer_dir("gold")),
    )

    print()
    print("=" * 72)
    print("[pipeline] DASHBOARD queries (Gold only)")
    print("=" * 72)
    print("SQL Dashboard: paste each query from src/dashboard/dashboard_queries.sql")
    print("into its own widget. Field mappings: src/dashboard/DASHBOARD_GUIDE.md")
    sql_text = rt.qualify_sql(rt.read_text("dashboard/dashboard_queries.sql"))
    for title, stmt in _split_dashboard_selects(sql_text):
        print()
        print(title)
        df = spark.sql(stmt)
        rt.display_df(df, title=None)

    print()
    print("[pipeline] complete")
    print(f"  {rt.table_name('bronze.customers')} / {rt.table_name('bronze.orders')} / {rt.table_name('bronze.products')}")
    print(f"  {rt.table_name('silver.customers')} / {rt.table_name('silver.orders')} / {rt.table_name('silver.products')} / {rt.table_name('silver.quality_metrics')}")
    print(f"  {rt.table_name('gold.sales_by_product')} / {rt.table_name('gold.revenue_by_customer')} / {rt.table_name('gold.customer_segmentation')}")


if __name__ == "__main__":
    run_pipeline()
