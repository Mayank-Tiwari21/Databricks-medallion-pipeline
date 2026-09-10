"""
Bronze ingest — orders.csv → bronze.orders

Raw ingest only. No cleaning, filtering, or deduplication.
Null FKs, orphan FKs, and duplicate order_ids from the generator must
survive this step so Silver can FLAG them.

Databricks (notebook or Repo job):
    Set widget `source_path` to the CSV on a Unity Catalog volume.
    Databricks Free Edition has no DBFS / FileStore.

    Default:
        /Volumes/workspace/default/ecommerce/orders.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
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
#   1. 100 customer_id values and 200 product_id values are empty. inferSchema
#      often types those columns as DoubleType (nulls + integers) which would
#      store 477 as 477.0 and confuse Silver uniqueness / RI checks.
#   2. payment_date is legitimately null on Pending rows; inferSchema may
#      still get DateType, but we do not want a surprise StringType.
#   3. All fields nullable=True so PERMISSIVE reads never drop a dirty row
#      that Silver is supposed to flag.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("order_status", StringType(), True),
        StructField("payment_date", DateType(), True),
    ]
)

DEFAULT_SOURCE_PATH = "/Volumes/workspace/default/ecommerce/orders.csv"
DEFAULT_TARGET_TABLE = "bronze.orders"


def _get_param(name: str, default: str) -> str:
    """Databricks widget if dbutils exists; otherwise the default."""
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ingest_orders(
    spark: SparkSession,
    source_path: str,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> int:
    """Read orders.csv and write bronze.orders. Returns rows written."""
    print(f"[bronze.orders] source_path = {source_path}")
    print(f"[bronze.orders] target_table = {target_table}")

    # PERMISSIVE: keep rows that do not parse (do not drop them).
    # nullValue="": generator writes NULL FKs / Pending payment_date as "".
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(ORDERS_SCHEMA)
        .load(source_path)
    )

    rows_read = raw_df.count()
    print(f"[bronze.orders] rows read (before write) = {rows_read}")

    bronze_df = raw_df.withColumn(
        "_source_file", F.input_file_name()
    ).withColumn("_ingested_at", F.current_timestamp())

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    rows_written = spark.table(target_table).count()
    print(f"[bronze.orders] rows written (after write) = {rows_written}")
    if rows_read != rows_written:
        print(
            "[bronze.orders] WARNING: row count changed on write. "
            "Bronze must not filter or dedupe — investigate."
        )
    return rows_written


if __name__ == "__main__":
    spark = _get_spark()
    source_path = _get_param("source_path", DEFAULT_SOURCE_PATH)
    target_table = _get_param("target_table", DEFAULT_TARGET_TABLE)
    ingest_orders(spark, source_path, target_table)
