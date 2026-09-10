"""
Silver — uniqueness checks.

Detect duplicate customers.customer_id and orders.order_id with a window
count (not a groupBy + join). Flag *every* row in a duplicate PK group,
not only the second occurrence. Never drop rows — append tokens onto
`quality_check_result`.

Databricks:
    Defaults read/write silver.* (run 01_quality_completeness.py first so
    completeness flags are preserved). Override with widgets
    `customers_source`, `orders_source`, `customers_target`, `orders_target`.

Expected seeded failures (from data generation):
    customers.customer_id  10 IDs appear twice → 20 rows flagged
    orders.order_id        20 IDs appear twice → 40 rows flagged
    unique-id %            ~99.90% customers, ~99.98% orders
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

FLAG_DUP_CUSTOMER_ID = "uniqueness.customer_id_duplicate"
FLAG_DUP_ORDER_ID = "uniqueness.order_id_duplicate"

# Read silver so completeness tokens stay on the row. Fall back to bronze
# if this script is run first (quality_check_result will be created).
DEFAULT_CUSTOMERS_SOURCE = "workspace.default.silver_customers"
DEFAULT_ORDERS_SOURCE = "workspace.default.silver_orders"
DEFAULT_CUSTOMERS_TARGET = "workspace.default.silver_customers"
DEFAULT_ORDERS_TARGET = "workspace.default.silver_orders"

PK_COUNT_COL = "_pk_occurrence_count"

SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("pk_field", StringType(), False),
        StructField("total_rows", IntegerType(), False),
        StructField("distinct_pk_count", IntegerType(), False),
        StructField("duplicate_pk_values", IntegerType(), False),
        StructField("rows_in_duplicate_groups", IntegerType(), False),
        StructField("pct_unique_ids", DoubleType(), False),
        StructField("flag_token", StringType(), False),
    ]
)


def _get_param(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 — Databricks only
        return dbutils.widgets.get(name)  # noqa: F821
    except NameError:
        return default


def _get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _empty_flags() -> F.Column:
    return F.array().cast("array<string>")


def _ensure_flag_column(df: DataFrame) -> DataFrame:
    """Completeness may not have run; uniqueness still needs the array."""
    if "quality_check_result" in df.columns:
        return df
    return df.withColumn("quality_check_result", _empty_flags())


def flag_duplicate_pk(
    df: DataFrame,
    pk_column: str,
    flag_token: str,
) -> DataFrame:
    """Flag every row whose non-null PK appears more than once.

    WHY a window, not groupBy + join
    --------------------------------
    The naive pattern is:
        dups = df.groupBy(pk).count().filter(count > 1)
        df.join(dups, pk)
    That is two shuffles (aggregate, then join back to 100k+ rows) and a
    wider plan. `count(*) OVER (PARTITION BY pk)` is one shuffle on the PK
    and the count is already on every row — including the *first*
    occurrence, which a dropDuplicates approach would keep unflagged.
    """
    df = _ensure_flag_column(df)

    # All null PKs land in one window partition in Spark. Uniqueness of
    # NULL is not defined; completeness already flags null keys. Ignore
    # nulls here so they are not treated as one giant duplicate group.
    window_spec = Window.partitionBy(pk_column)
    with_counts = df.withColumn(PK_COUNT_COL, F.count(F.lit(1)).over(window_spec))

    is_duplicate = F.col(pk_column).isNotNull() & (F.col(PK_COUNT_COL) > 1)
    new_flags = F.when(is_duplicate, F.array(F.lit(flag_token))).otherwise(
        _empty_flags()
    )

    # Append — do not overwrite completeness tokens.
    return (
        with_counts.withColumn(
            "quality_check_result",
            F.concat(
                F.coalesce(F.col("quality_check_result"), _empty_flags()),
                new_flags,
            ),
        ).drop(PK_COUNT_COL)
    )


def _uniqueness_metrics(
    df: DataFrame,
    table_name: str,
    pk_column: str,
    flag_token: str,
) -> dict:
    total = df.count()
    distinct_pk = (
        df.select(pk_column).where(F.col(pk_column).isNotNull()).distinct().count()
    )
    flagged = df.filter(F.array_contains(F.col("quality_check_result"), flag_token))
    n_flagged = flagged.count()
    n_dup_values = flagged.select(pk_column).distinct().count() if n_flagged else 0
    pct_unique = round((distinct_pk / total) * 100.0, 2) if total else 0.0
    return {
        "table_name": table_name,
        "pk_field": pk_column,
        "total_rows": total,
        "distinct_pk_count": distinct_pk,
        "duplicate_pk_values": n_dup_values,
        "rows_in_duplicate_groups": n_flagged,
        "pct_unique_ids": pct_unique,
        "flag_token": flag_token,
    }


def _print_summary(spark: SparkSession, rows: list[dict]) -> DataFrame:
    print()
    print("=" * 88)
    print("Silver uniqueness summary")
    print("=" * 88)
    header = (
        f"{'table':<20} {'pk':<14} {'rows':>8} {'distinct':>10} "
        f"{'dup ids':>8} {'flagged':>8} {'% unique':>10}  flag"
    )
    print(header)
    print("-" * 88)
    for row in rows:
        print(
            f"{row['table_name']:<20} {row['pk_field']:<14} "
            f"{row['total_rows']:>8,} {row['distinct_pk_count']:>10,} "
            f"{row['duplicate_pk_values']:>8,} {row['rows_in_duplicate_groups']:>8,} "
            f"{row['pct_unique_ids']:>9.2f}%  {row['flag_token']}"
        )
    print("-" * 88)
    print("All rows in a duplicate PK group are flagged (not only the 2nd).")
    print("Expected: 10 duplicate customer_ids (20 rows), 20 duplicate order_ids (40 rows).")
    print("pct_unique_ids = distinct(pk) / row_count. Rows are FLAGGED only — nothing dropped.")
    print("=" * 88)

    summary_df = spark.createDataFrame(
        [
            (
                r["table_name"],
                r["pk_field"],
                r["total_rows"],
                r["distinct_pk_count"],
                r["duplicate_pk_values"],
                r["rows_in_duplicate_groups"],
                r["pct_unique_ids"],
                r["flag_token"],
            )
            for r in rows
        ],
        schema=SUMMARY_SCHEMA,
    )
    summary_df.show(truncate=False)
    return summary_df


def _write_silver(df: DataFrame, target_table: str) -> None:
    """Materialize via a staging table so we never persist() (serverless)."""
    spark = df.sparkSession
    staging = f"{target_table}_stg"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging)
    )
    (
        spark.table(staging)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    spark.sql(f"DROP TABLE IF EXISTS {staging}")


def run_uniqueness_checks(
    spark: SparkSession,
    customers_source: str = DEFAULT_CUSTOMERS_SOURCE,
    orders_source: str = DEFAULT_ORDERS_SOURCE,
    customers_target: str = DEFAULT_CUSTOMERS_TARGET,
    orders_target: str = DEFAULT_ORDERS_TARGET,
) -> DataFrame:
    """Flag duplicate PKs, write Silver tables, return uniqueness summary."""
    print(f"[silver.uniqueness] customers {customers_source} → {customers_target}")
    print(f"[silver.uniqueness] orders    {orders_source} → {orders_target}")

    customers_in = spark.table(customers_source)
    orders_in = spark.table(orders_source)

    rows_c_before = customers_in.count()
    rows_o_before = orders_in.count()
    print(f"[silver.uniqueness] customers rows in  = {rows_c_before:,}")
    print(f"[silver.uniqueness] orders    rows in  = {rows_o_before:,}")

    customers_out = flag_duplicate_pk(
        customers_in, "customer_id", FLAG_DUP_CUSTOMER_ID
    )
    orders_out = flag_duplicate_pk(orders_in, "order_id", FLAG_DUP_ORDER_ID)

    summary_rows = [
        _uniqueness_metrics(
            customers_out, customers_target, "customer_id", FLAG_DUP_CUSTOMER_ID
        ),
        _uniqueness_metrics(
            orders_out, orders_target, "order_id", FLAG_DUP_ORDER_ID
        ),
    ]

    _write_silver(customers_out, customers_target)
    _write_silver(orders_out, orders_target)

    rows_c_after = spark.table(customers_target).count()
    rows_o_after = spark.table(orders_target).count()
    print(f"[silver.uniqueness] customers rows out = {rows_c_after:,}")
    print(f"[silver.uniqueness] orders    rows out = {rows_o_after:,}")
    if rows_c_before != rows_c_after or rows_o_before != rows_o_after:
        print(
            "[silver.uniqueness] WARNING: row count changed. "
            "Silver must not drop rows — investigate."
        )

    return _print_summary(spark, summary_rows)


if __name__ == "__main__":
    spark = _get_spark()
    run_uniqueness_checks(
        spark,
        customers_source=_get_param("customers_source", DEFAULT_CUSTOMERS_SOURCE),
        orders_source=_get_param("orders_source", DEFAULT_ORDERS_SOURCE),
        customers_target=_get_param("customers_target", DEFAULT_CUSTOMERS_TARGET),
        orders_target=_get_param("orders_target", DEFAULT_ORDERS_TARGET),
    )
