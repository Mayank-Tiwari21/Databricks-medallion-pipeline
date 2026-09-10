"""
Bronze ingest — customers.csv → bronze.customers

Raw ingest only. No cleaning, filtering, or deduplication.
Null emails and duplicate customer_ids from the generator must survive
this step so Silver can FLAG them.

Databricks (notebook or Repo job):
    CSVs are read from the Workspace `data/` folder with Python.
    Spark file sources are not used (Free Edition prefixes dbfs: and fails).
    Tables: workspace.default.bronze_customers
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
#   1. 50 emails are empty. inferSchema still types email as string, but empty
#      STRING cells stay "" unless we declare nullValue — completeness in
#      Silver looks for null, not empty string.
#   2. Duplicate customer_ids do not break inference, but we still want an
#      IntegerType PK. inferSchema can promote to LongType/DoubleType.
#   3. Every column is nullable=True. A non-nullable IntegerType on
#      customer_id would not drop duplicates, but it WOULD drop a future null
#      PK. Bronze must not drop rows.
CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", DecimalType(10, 2), True),
    ]
)

DEFAULT_SOURCE_PATH = "/Volumes/workspace/default/ecommerce/customers.csv"
DEFAULT_TARGET_TABLE = "workspace.default.bronze_customers"


def _get_param(name: str, default: str) -> str:
    """Databricks widget if dbutils exists; otherwise the default."""
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ingest_customers(
    spark: SparkSession,
    source_path: str,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> int:
    """Read customers.csv and write bronze.customers. Returns rows written."""
    print(f"[bronze.customers] source_path = {source_path}")
    print(f"[bronze.customers] target_table = {target_table}")

    # Python CSV read from the Workspace file. Spark.read.csv would rewrite
    # the path to dbfs:/... which Free Edition cannot use.
    try:
        from databricks_runtime import read_csv_workspace

        raw_df = read_csv_workspace(spark, source_path, CUSTOMERS_SCHEMA)
    except ImportError:
        raise ImportError("databricks_runtime.read_csv_workspace is required on Free Edition")

    rows_read = raw_df.count()
    print(f"[bronze.customers] rows read (before write) = {rows_read}")

    bronze_df = raw_df.withColumn("_source_file", F.lit(source_path)).withColumn(
        "_ingested_at", F.current_timestamp()
    )

    # Free Edition cannot CREATE SCHEMA on catalog workspace. Write into
    # the existing default schema (workspace.default.bronze_customers).

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    rows_written = spark.table(target_table).count()
    print(f"[bronze.customers] rows written (after write) = {rows_written}")
    if rows_read != rows_written:
        print(
            "[bronze.customers] WARNING: row count changed on write. "
            "Bronze must not filter or dedupe — investigate."
        )
    return rows_written


if __name__ == "__main__":
    spark = _get_spark()
    source_path = _get_param("source_path", DEFAULT_SOURCE_PATH)
    target_table = _get_param("target_table", DEFAULT_TARGET_TABLE)
    ingest_customers(spark, source_path, target_table)
