"""
Bronze ingest — products.csv → bronze.products

Raw ingest only. No cleaning, filtering, or deduplication.
The product file is the clean control dimension; we still use an explicit
schema so types match orders.product_id (IntegerType, not inferred Long).

Databricks (notebook or Repo job):
    CSVs are read from the Workspace `data/` folder with Python.
    Spark file sources are not used (Free Edition prefixes dbfs: and fails).
    Tables: workspace.default.bronze_products
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Explicit schema
# ---------------------------------------------------------------------------
# WHY not inferSchema=True:
#   1. price/cost must be DECIMAL(10,2) to match orders.unit_price. inferSchema
#      typically yields DoubleType, which is a poor match for money.
#   2. product_id must be IntegerType so Silver RI can join to orders.product_id
#      without a silent long/int mismatch.
#   3. nullable=True on every column — Bronze never rejects a row on null.
PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("cost", DecimalType(10, 2), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
    ]
)

DEFAULT_SOURCE_PATH = "/Volumes/workspace/default/ecommerce/products.csv"
DEFAULT_TARGET_TABLE = "workspace.default.bronze_products"


def _get_param(name: str, default: str) -> str:
    """Databricks widget if dbutils exists; otherwise the default."""
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ingest_products(
    spark: SparkSession,
    source_path: str,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> int:
    """Read products.csv and write bronze.products. Returns rows written."""
    print(f"[bronze.products] source_path = {source_path}")
    print(f"[bronze.products] target_table = {target_table}")

    try:
        from databricks_runtime import read_csv_workspace

        raw_df = read_csv_workspace(spark, source_path, PRODUCTS_SCHEMA)
    except ImportError:
        raise ImportError("databricks_runtime.read_csv_workspace is required on Free Edition")

    rows_read = raw_df.count()
    print(f"[bronze.products] rows read (before write) = {rows_read}")

    bronze_df = raw_df.withColumn("_source_file", F.lit(source_path)).withColumn(
        "_ingested_at", F.current_timestamp()
    )

    # Free Edition cannot CREATE SCHEMA on catalog workspace.

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    rows_written = spark.table(target_table).count()
    print(f"[bronze.products] rows written (after write) = {rows_written}")
    if rows_read != rows_written:
        print(
            "[bronze.products] WARNING: row count changed on write. "
            "Bronze must not filter or dedupe — investigate."
        )
    return rows_written


if __name__ == "__main__":
    spark = _get_spark()
    source_path = _get_param("source_path", DEFAULT_SOURCE_PATH)
    target_table = _get_param("target_table", DEFAULT_TARGET_TABLE)
    ingest_products(spark, source_path, target_table)
