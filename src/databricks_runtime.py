"""
Databricks-safe path and module loading.

Notebooks (ipykernel) do not set __file__. This module is loaded from disk
via importlib so *it* has __file__, and every other layer can resolve
bronze / silver / gold / dashboard files from src/.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Databricks Free Edition disables DBFS root (/FileStore, /dbfs).
# Spark reads landing CSVs from a Unity Catalog volume instead.
DEFAULT_SOURCE_DIR = "/Volumes/workspace/default/ecommerce"
LANDING_CSV_NAMES = ("customers.csv", "orders.csv", "products.csv")


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
    """Ignore leftover DBFS widget values from older notebooks."""
    raw = (source_dir or "").strip().rstrip("/")
    if (
        not raw
        or raw.startswith("/FileStore")
        or raw.startswith("/dbfs")
        or raw.startswith("dbfs:")
    ):
        print(
            f"[landing] ignoring DBFS path {source_dir!r}; "
            f"using {DEFAULT_SOURCE_DIR}"
        )
        return DEFAULT_SOURCE_DIR
    return raw


def repo_data_dir(src: Path | None = None) -> Path | None:
    """Repo `data/` folder (Workspace/Git files). Spark often cannot read these
    on Free Edition; Python Path / shutil still can."""
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


def ensure_uc_volume(spark: SparkSession, volume_dir: str) -> None:
    """CREATE VOLUME IF NOT EXISTS for /Volumes/<catalog>/<schema>/<volume>/..."""
    parts = [p for p in volume_dir.strip("/").split("/") if p]
    if len(parts) < 4 or parts[0] != "Volumes":
        return
    catalog, schema, volume = parts[1], parts[2], parts[3]
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`"
    )
    print(f"[landing] volume = {catalog}.{schema}.{volume}")


def stage_landing_csvs(
    spark: SparkSession,
    source_dir: str,
    src: Path | None = None,
) -> str:
    """Put the three CSVs on a UC Volume so Spark can ingest them.

    Free Edition has no DBFS. Preferred sources, in order:
      1. CSVs already in source_dir (Catalog Explorer upload)
      2. Copy from this repo's data/ (Workspace/Git) into the volume
    """
    source_dir = normalize_source_dir(source_dir)
    print(f"[landing] source_dir = {source_dir}")
    try:
        ensure_uc_volume(spark, source_dir)
    except Exception as exc:
        print(f"[landing] CREATE VOLUME skipped: {type(exc).__name__}: {exc}")

    dest = Path(source_dir)
    present = all((dest / name).is_file() for name in LANDING_CSV_NAMES)
    data_dir = repo_data_dir(src)

    if data_dir is not None:
        dest.mkdir(parents=True, exist_ok=True)
        for name in LANDING_CSV_NAMES:
            shutil.copy2(data_dir / name, dest / name)
            print(f"[landing] copied {name} → {dest / name}")
        return source_dir

    if present:
        print(f"[landing] using CSVs already in {source_dir}")
        return source_dir

    raise FileNotFoundError(
        "Landing CSVs not found. Databricks Free Edition cannot use DBFS "
        "(/FileStore). Either:\n"
        "  1. Keep data/customers.csv, data/orders.csv, data/products.csv in "
        "the Git/Workspace folder next to src/ (the driver copies them to the "
        "volume), or\n"
        "  2. Catalog → workspace → default → Create volume `ecommerce` → "
        "Upload the three CSVs, then set widget source_dir to "
        f"{DEFAULT_SOURCE_DIR}"
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
