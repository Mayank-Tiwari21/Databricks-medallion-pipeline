"""
Databricks-safe path and module loading.

Notebooks (ipykernel) do not set __file__. This module is loaded from disk
via importlib so *it* has __file__, and every other layer can resolve
bronze / silver / gold / dashboard files from src/.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

# Databricks Free Edition: no DBFS. Spark.read.csv on /Volumes is rewritten
# to dbfs:/Volumes/... and fails. Bronze reads Workspace data/*.csv with
# Python and writes Delta tables in workspace.default.
DEFAULT_SOURCE_DIR = "workspace-data"
LANDING_CSV_NAMES = ("customers.csv", "orders.csv", "products.csv")

# Free Edition: no CREATE SCHEMA on catalog `workspace`. Tables live in the
# existing `default` schema as bronze_customers, silver_orders, ...
DEFAULT_UC_CATALOG = "workspace"
DEFAULT_UC_SCHEMA = "default"

LOGICAL_TABLES = {
    "bronze.customers": "bronze_customers",
    "bronze.orders": "bronze_orders",
    "bronze.products": "bronze_products",
    "silver.customers": "silver_customers",
    "silver.orders": "silver_orders",
    "silver.products": "silver_products",
    "silver.quality_metrics": "silver_quality_metrics",
    "gold.sales_by_product": "gold_sales_by_product",
    "gold.revenue_by_customer": "gold_revenue_by_customer",
    "gold.customer_segmentation": "gold_customer_segmentation",
}


def uc_catalog() -> str:
    return get_param("uc_catalog", DEFAULT_UC_CATALOG).strip() or DEFAULT_UC_CATALOG


def uc_schema() -> str:
    return get_param("uc_schema", DEFAULT_UC_SCHEMA).strip() or DEFAULT_UC_SCHEMA


def table_name(logical: str) -> str:
    """bronze.customers → workspace.default.bronze_customers."""
    suffix = LOGICAL_TABLES.get(logical, logical.replace(".", "_"))
    return f"{uc_catalog()}.{uc_schema()}.{suffix}"


def qualify_sql(script: str) -> str:
    """Rewrite bronze.* / silver.* / gold.* to UC default-schema tables.

    Also drops CREATE DATABASE / CREATE SCHEMA (Free Edition cannot create
    schemas on catalog workspace).
    """
    kept: list[str] = []
    for line in script.splitlines():
        stripped = line.lstrip().upper()
        if stripped.startswith("CREATE DATABASE") or stripped.startswith("CREATE SCHEMA"):
            continue
        kept.append(line)
    script = "\n".join(kept)
    for logical in sorted(LOGICAL_TABLES, key=len, reverse=True):
        script = script.replace(logical, table_name(logical))
    return script


def get_param(name: str, default: str = "") -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def notebook_folder_candidates() -> list[Path]:
    """Folders implied by the running Databricks notebook path."""
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
    notebooks = [
        Path("/Workspace") / rel,
        Path("/") / rel,
    ]
    folders: list[Path] = []
    for notebook in notebooks:
        parent = notebook.parent
        folders.extend(
            [
                parent,
                parent / "src",
                parent.parent / "src",
            ]
        )
    return folders


def src_root() -> Path:
    """Directory that contains bronze/, silver/, gold/, dashboard/."""
    # When this file was loaded from disk, __file__ is reliable.
    here = Path(__file__).resolve().parent
    if (here / "bronze" / "01_ingest_customers.py").is_file():
        return here

    marker = Path("bronze") / "01_ingest_customers.py"
    candidates: list[Path] = []

    candidates.extend(notebook_folder_candidates())

    widget = get_param("src_root", "")
    if widget.strip():
        root = Path(widget.strip())
        candidates.extend(
            [
                root,
                root / "src",
                root / "databricks-medallion-pipeline" / "src",
            ]
        )

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
            return directory

    raise FileNotFoundError(
        "Cannot find src/bronze/01_ingest_customers.py. Set widget src_root "
        "to the pipeline src folder, e.g. "
        "/Workspace/Users/<you>/DE-C1-project/databricks-medallion-pipeline/src"
    )


def layer_dir(layer: str) -> Path:
    return src_root() / layer


def load_py(relative: str, module_name: str):
    """Load a .py under src/ even when its name starts with a digit."""
    path = src_root() / relative
    if not path.is_file():
        raise FileNotFoundError(f"Python module not found: {path}")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    return module


def read_text(relative: str) -> str:
    path = src_root() / relative
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    return path.read_text(encoding="utf-8")


def normalize_source_dir(source_dir: str) -> str:
    """Prefer Workspace data/. Ignore DBFS and Volume paths (Spark prefixes dbfs:)."""
    raw = (source_dir or "").strip().rstrip("/")
    data = repo_data_dir()
    blocked = (
        not raw
        or raw == "workspace-data"
        or raw.startswith("/FileStore")
        or raw.startswith("/dbfs")
        or raw.startswith("dbfs:")
        or raw.startswith("/Volumes")
    )
    if blocked and data is not None:
        print(f"[landing] using Workspace CSVs at {data}")
        return str(data)
    if data is not None:
        return str(data)
    return raw


def repo_data_dir(src: Path | None = None) -> Path | None:
    """Repo `data/` folder (Workspace/Git files). Spark often cannot read these
    on Free Edition; Python still can."""
    roots: list[Path] = []
    if src is not None:
        roots.append(Path(src))
    try:
        roots.append(src_root())
    except Exception:
        pass
    for root in roots:
        candidate = Path(root).resolve().parent / "data"
        if all((candidate / name).is_file() for name in LANDING_CSV_NAMES):
            return candidate
    return None


def parse_csv_cell(raw: str | None, data_type):
    """PERMISSIVE: unparsable values become None; the row is kept."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "":
        return None
    try:
        if isinstance(data_type, (IntegerType, LongType)):
            return int(float(text))
        if isinstance(data_type, DoubleType):
            return float(text)
        if isinstance(data_type, DecimalType):
            return Decimal(text)
        if isinstance(data_type, DateType):
            return date.fromisoformat(text[:10])
        if isinstance(data_type, StringType):
            return text
        return text
    except (ValueError, InvalidOperation, TypeError):
        return None


def read_csv_workspace(spark: SparkSession, path: str, schema: StructType):
    """Read a CSV with Python from a Workspace file. Do not use Spark file
    sources — Free Edition rewrites /Volumes and /Workspace to dbfs: and fails.
    """
    csv_path = Path(path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Workspace CSV not found: {csv_path}")
    print(f"[landing] Python CSV read (no Spark/DBFS): {csv_path}")

    names = [field.name for field in schema.fields]
    rows: list[tuple] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for rec in reader:
            rows.append(
                tuple(
                    parse_csv_cell(rec.get(name), schema[name].dataType)
                    for name in names
                )
            )
    print(f"[landing] parsed {len(rows):,} rows")
    if not rows:
        return spark.createDataFrame([], schema=schema)

    # Spark Connect cannot take a Volume/DBFS file scan. Local createDataFrame
    # in modest batches avoids a single huge RPC payload (orders.csv is 100k).
    batch_size = 20_000
    frames = []
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        frames.append(spark.createDataFrame(chunk, schema=schema))
        print(f"[landing] loaded rows {start + 1:,}–{start + len(chunk):,}")
    out = frames[0]
    for extra in frames[1:]:
        out = out.unionByName(extra)
    return out


def stage_landing_csvs(
    spark: SparkSession,
    source_dir: str,
    src: Path | None = None,
) -> str:
    """Return the Workspace `data/` folder. Never copy to Volumes or DBFS."""
    data_dir = repo_data_dir(src)
    if data_dir is not None:
        print(f"[landing] Workspace data dir = {data_dir}")
        return str(data_dir)

    source_dir = (source_dir or "").rstrip("/")
    candidate = Path(source_dir) if source_dir else None
    if candidate is not None and all(
        (candidate / name).is_file() for name in LANDING_CSV_NAMES
    ):
        print(f"[landing] using {candidate}")
        return str(candidate)

    raise FileNotFoundError(
        "Landing CSVs not found in the Workspace folder. Keep "
        "data/customers.csv, data/orders.csv, data/products.csv next to src/ "
        "(Git / Workspace import). Spark cannot read /Volumes or DBFS on "
        "Free Edition — Bronze loads those files with Python and writes "
        "workspace.default.bronze_* Delta tables."
    )


def display_df(df, title: str | None = None) -> None:
    """Render a DataFrame in Databricks; fall back to show() elsewhere.

    `display()` is a notebook builtin on `__main__`, not on imported modules.
    Databricks Spark DataFrames also expose `.display()`.
    """
    if title:
        print(title)
    if hasattr(df, "display"):
        df.display()
        return
    main = sys.modules.get("__main__")
    builtin_display = getattr(main, "display", None) if main is not None else None
    if callable(builtin_display):
        builtin_display(df)
        return
    df.show(truncate=False)
