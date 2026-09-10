"""
Silver orchestrator — run every quality check, merge flags, write tables.

Reads Bronze, applies 01–05 flag functions in memory (so each row ends with
one `quality_check_result` array), writes:

    silver.customers
    silver.orders
    silver.products
    silver.quality_metrics   ← queryable pass-rate report

ALL rows are preserved, including flagged ones. Gold should filter
`size(quality_check_result) = 0` rather than this script dropping them.

    Databricks:
        Run after Bronze ingest. Widget overrides for bronze_*/silver_* names.
        Preferred: src/run_pipeline.py (loads this file from disk so 01–05
        resolve without __file__). Standalone: set widget src_root.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

def _t(logical: str) -> str:
    try:
        from databricks_runtime import table_name

        return table_name(logical)
    except Exception:
        return "workspace.default." + logical.replace(".", "_")


DEFAULT_BRONZE_CUSTOMERS = _t("bronze.customers")
DEFAULT_BRONZE_ORDERS = _t("bronze.orders")
DEFAULT_BRONZE_PRODUCTS = _t("bronze.products")
DEFAULT_SILVER_CUSTOMERS = _t("silver.customers")
DEFAULT_SILVER_ORDERS = _t("silver.orders")
DEFAULT_SILVER_PRODUCTS = _t("silver.products")
DEFAULT_METRICS_TABLE = _t("silver.quality_metrics")

METRICS_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_group", StringType(), False),
        StructField("flag_token", StringType(), True),
        StructField("total_rows", IntegerType(), False),
        StructField("failed_rows", IntegerType(), False),
        StructField("passed_rows", IntegerType(), False),
        StructField("pass_pct", DoubleType(), False),
    ]
)

# One row in silver.quality_metrics per (table, check).
# flag_token is null for the overall "row has zero flags" rollup.
CHECK_CATALOG = [
    {
        "table_name": "silver.customers",
        "check_name": "email is not null/blank",
        "check_group": "completeness",
        "flag_token": "completeness.email_is_null",
    },
    {
        "table_name": "silver.customers",
        "check_name": "customer_id is unique",
        "check_group": "uniqueness",
        "flag_token": "uniqueness.customer_id_duplicate",
    },
    {
        "table_name": "silver.customers",
        "check_name": "signup_date is not in the future",
        "check_group": "business",
        "flag_token": "business.signup_date_in_future",
    },
    {
        "table_name": "silver.customers",
        "check_name": "row passed all checks",
        "check_group": "overall",
        "flag_token": None,
    },
    {
        "table_name": "silver.orders",
        "check_name": "customer_id is not null",
        "check_group": "completeness",
        "flag_token": "completeness.customer_id_is_null",
    },
    {
        "table_name": "silver.orders",
        "check_name": "product_id is not null",
        "check_group": "completeness",
        "flag_token": "completeness.product_id_is_null",
    },
    {
        "table_name": "silver.orders",
        "check_name": "order_id is unique",
        "check_group": "uniqueness",
        "flag_token": "uniqueness.order_id_duplicate",
    },
    {
        "table_name": "silver.orders",
        "check_name": "total_amount == quantity * unit_price",
        "check_group": "type",
        "flag_token": "type.total_amount_neq_qty_times_price",
    },
    {
        "table_name": "silver.orders",
        "check_name": "customer_id exists in customers",
        "check_group": "referential_integrity",
        "flag_token": "ri.customer_id_orphan",
    },
    {
        "table_name": "silver.orders",
        "check_name": "product_id exists in products",
        "check_group": "referential_integrity",
        "flag_token": "ri.product_id_orphan",
    },
    {
        "table_name": "silver.orders",
        "check_name": "order_date is not in the future",
        "check_group": "business",
        "flag_token": "business.order_date_in_future",
    },
    {
        "table_name": "silver.orders",
        "check_name": "row passed all checks",
        "check_group": "overall",
        "flag_token": None,
    },
    {
        "table_name": "silver.products",
        "check_name": "row passed all checks",
        "check_group": "overall",
        "flag_token": None,
    },
]


def _get_param(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _scripts_dir() -> Path:
    """Folder that contains 01_quality_completeness.py (notebook-safe)."""
    try:
        from databricks_runtime import layer_dir

        return layer_dir("silver")
    except Exception:
        pass

    marker = "01_quality_completeness.py"
    candidates: list[Path] = []

    file_val = globals().get("__file__")
    if file_val:
        candidates.append(Path(file_val).resolve().parent)

    widget_dir = _get_param("src_root", "") or _get_param("silver_src_dir", "")
    if widget_dir.strip():
        root = Path(widget_dir.strip())
        candidates.extend(
            [
                root,
                root / "silver",
                root / "src" / "silver",
                root / "databricks-medallion-pipeline" / "src" / "silver",
            ]
        )

    cwd = Path.cwd()
    candidates.extend(
        [
            cwd,
            cwd / "src" / "silver",
            cwd / "databricks-medallion-pipeline" / "src" / "silver",
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
            print(f"[silver] quality scripts dir = {directory}")
            return directory

    raise FileNotFoundError(
        "Cannot find src/silver/01_quality_completeness.py. Databricks "
        "notebooks have no __file__. Run src/run_pipeline.py, or set widget "
        "src_root to the src folder."
    )


def _load_sibling(filename: str, module_name: str):
    """Load 01_*.py etc. by path — names starting with a digit are not importable."""
    try:
        from databricks_runtime import load_py

        return load_py(f"silver/{filename}", module_name)
    except Exception:
        pass

    path = _scripts_dir() / filename
    if not path.is_file():
        raise FileNotFoundError(f"Quality script not found: {path}")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _empty_flags():
    return F.array().cast("array<string>")


def _write_delta(df: DataFrame, target_table: str) -> None:
    # Free Edition: no CREATE SCHEMA. Tables are workspace.default.silver_*.
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def apply_all_customer_checks(customers: DataFrame, completeness, uniqueness, business) -> DataFrame:
    """Merge completeness → uniqueness → business flags on customers."""
    out = completeness.flag_customer_completeness(customers)
    out = uniqueness.flag_duplicate_pk(
        out, "customer_id", uniqueness.FLAG_DUP_CUSTOMER_ID
    )
    out = business.flag_customer_business_rules(out)
    return out


def apply_all_order_checks(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    completeness,
    uniqueness,
    type_val,
    ri,
    business,
) -> DataFrame:
    """Merge completeness → uniqueness → type → RI → business flags on orders."""
    out = completeness.flag_order_completeness(orders)
    out = uniqueness.flag_duplicate_pk(out, "order_id", uniqueness.FLAG_DUP_ORDER_ID)
    out = type_val.flag_amount_identity(out)
    out, _orphan_c, _orphan_p = ri.flag_order_referential_integrity(
        out, customers, products
    )
    out = business.flag_order_business_rules(out)
    return out


def apply_product_passthrough(products: DataFrame) -> DataFrame:
    """Products have no seeded DQ checks — empty flag array, all rows kept."""
    if "quality_check_result" in products.columns:
        return products
    return products.withColumn("quality_check_result", _empty_flags())


def _failed_count(df: DataFrame, flag_token: str | None) -> int:
    if flag_token is None:
        return df.filter(F.size(F.col("quality_check_result")) > 0).count()
    return df.filter(
        F.array_contains(F.col("quality_check_result"), flag_token)
    ).count()


def build_quality_metrics(
    spark: SparkSession,
    tables: dict[str, DataFrame],
) -> DataFrame:
    """One row per (table, check) with pass_pct. Query as silver.quality_metrics."""
    rows = []
    for spec in CHECK_CATALOG:
        df = tables[spec["table_name"]]
        total = df.count()
        failed = _failed_count(df, spec["flag_token"])
        passed = total - failed
        pass_pct = round((passed / total) * 100.0, 4) if total else 0.0
        rows.append(
            (
                spec["table_name"],
                spec["check_name"],
                spec["check_group"],
                spec["flag_token"],
                total,
                failed,
                passed,
                pass_pct,
            )
        )
    return spark.createDataFrame(rows, schema=METRICS_SCHEMA)


def _print_metrics(metrics_df: DataFrame) -> None:
    print()
    print("=" * 100)
    print("Silver quality metrics  (also saved to silver.quality_metrics)")
    print("=" * 100)
    metrics_df.orderBy("table_name", "check_group", "check_name").show(50, truncate=False)
    print("All Silver tables keep flagged rows. Gold: WHERE size(quality_check_result) = 0")
    print("=" * 100)


def create_silver_tables(
    spark: SparkSession,
    bronze_customers: str = DEFAULT_BRONZE_CUSTOMERS,
    bronze_orders: str = DEFAULT_BRONZE_ORDERS,
    bronze_products: str = DEFAULT_BRONZE_PRODUCTS,
    silver_customers: str = DEFAULT_SILVER_CUSTOMERS,
    silver_orders: str = DEFAULT_SILVER_ORDERS,
    silver_products: str = DEFAULT_SILVER_PRODUCTS,
    metrics_table: str = DEFAULT_METRICS_TABLE,
) -> DataFrame:
    """Run all checks, write silver.* including quality_metrics, return metrics DF."""
    completeness = _load_sibling("01_quality_completeness.py", "quality_completeness")
    uniqueness = _load_sibling("02_quality_uniqueness.py", "quality_uniqueness")
    type_val = _load_sibling("03_quality_type_validation.py", "quality_type")
    ri = _load_sibling("04_quality_referential_integrity.py", "quality_ri")
    business = _load_sibling("05_quality_business_logic.py", "quality_business")

    print("[silver] reading Bronze")
    customers_bronze = spark.table(bronze_customers)
    orders_bronze = spark.table(bronze_orders)
    products_bronze = spark.table(bronze_products)

    n_c = customers_bronze.count()
    n_o = orders_bronze.count()
    n_p = products_bronze.count()
    print(f"[silver] bronze row counts  customers={n_c:,}  orders={n_o:,}  products={n_p:,}")

    print("[silver] applying quality checks (flag, do not drop)")
    customers_silver = apply_all_customer_checks(
        customers_bronze, completeness, uniqueness, business
    )
    products_silver = apply_product_passthrough(products_bronze)
    # RI parents are the Bronze dimensions (same PKs as Silver after flag-only).
    orders_silver = apply_all_order_checks(
        orders_bronze,
        customers_bronze,
        products_bronze,
        completeness,
        uniqueness,
        type_val,
        ri,
        business,
    )

    customers_silver = customers_silver.persist()
    orders_silver = orders_silver.persist()
    products_silver = products_silver.persist()
    customers_silver.count()
    orders_silver.count()
    products_silver.count()

    metrics_df = build_quality_metrics(
        spark,
        {
            "silver.customers": customers_silver,
            "silver.orders": orders_silver,
            "silver.products": products_silver,
        },
    )
    metrics_df = metrics_df.persist()
    metrics_df.count()

    print("[silver] writing Delta tables (all rows, including flagged)")
    _write_delta(customers_silver, silver_customers)
    _write_delta(orders_silver, silver_orders)
    _write_delta(products_silver, silver_products)
    _write_delta(metrics_df, metrics_table)

    for n_before, table in (
        (n_c, silver_customers),
        (n_o, silver_orders),
        (n_p, silver_products),
    ):
        n_after = spark.table(table).count()
        print(f"[silver] {table}: {n_before:,} in → {n_after:,} out")
        if n_before != n_after:
            print(
                f"[silver] WARNING: {table} row count changed. "
                "Silver must not drop rows — investigate."
            )

    customers_silver.unpersist()
    orders_silver.unpersist()
    products_silver.unpersist()

    _print_metrics(metrics_df)
    metrics_df.unpersist()
    return spark.table(metrics_table)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    create_silver_tables(
        spark,
        bronze_customers=_get_param("bronze_customers", DEFAULT_BRONZE_CUSTOMERS),
        bronze_orders=_get_param("bronze_orders", DEFAULT_BRONZE_ORDERS),
        bronze_products=_get_param("bronze_products", DEFAULT_BRONZE_PRODUCTS),
        silver_customers=_get_param("silver_customers", DEFAULT_SILVER_CUSTOMERS),
        silver_orders=_get_param("silver_orders", DEFAULT_SILVER_ORDERS),
        silver_products=_get_param("silver_products", DEFAULT_SILVER_PRODUCTS),
        metrics_table=_get_param("metrics_table", DEFAULT_METRICS_TABLE),
    )
