"""
Silver — type / numeric validation.

Flag orders where total_amount is not equal to quantity * unit_price
(rounded to 2 decimal places). Never drop rows — append tokens onto
`quality_check_result`.

Bronze already stored these columns as INT / DECIMAL via an explicit
StructType. This check catches the silent DoubleType drift that
inferSchema would introduce (see bronze-layer prompt-3) and any row
where the identity does not hold.

Databricks:
    Defaults read/write silver.orders (run 01 and 02 first).

Expected seeded failures: 0 — the generator sets
total_amount = round(quantity * unit_price, 2).
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

FLAG_AMOUNT_MISMATCH = "type.total_amount_neq_qty_times_price"

DEFAULT_ORDERS_SOURCE = "workspace.default.silver_orders"
DEFAULT_ORDERS_TARGET = "workspace.default.silver_orders"

SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("total_rows", IntegerType(), False),
        StructField("failed_rows", IntegerType(), False),
        StructField("pass_pct", DoubleType(), False),
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
    if "quality_check_result" in df.columns:
        return df
    return df.withColumn("quality_check_result", _empty_flags())


def _amount_mismatch() -> F.Column:
    """True when qty, price, and amount are all present but identity fails.

    Round to 2 places so DECIMAL(10,2) compares to the CSV grain. A raw
    `quantity * unit_price == total_amount` on DoubleType would flag
    false mismatches from binary float (e.g. 1 * 16.9).
    """
    computed = F.round(F.col("quantity") * F.col("unit_price"), 2)
    all_present = (
        F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("total_amount").isNotNull()
    )
    return all_present & (computed != F.col("total_amount"))


def flag_amount_identity(orders: DataFrame) -> DataFrame:
    """Append type.total_amount_neq_qty_times_price. All rows kept."""
    orders = _ensure_flag_column(orders)
    new_flags = F.when(_amount_mismatch(), F.array(F.lit(FLAG_AMOUNT_MISMATCH))).otherwise(
        _empty_flags()
    )
    return orders.withColumn(
        "quality_check_result",
        F.concat(
            F.coalesce(F.col("quality_check_result"), _empty_flags()),
            new_flags,
        ),
    )


def _print_summary(spark: SparkSession, rows: list[dict]) -> DataFrame:
    print()
    print("=" * 88)
    print("Silver type-validation summary")
    print("=" * 88)
    header = (
        f"{'table':<20} {'check':<36} {'rows':>8} {'failed':>8} "
        f"{'pass %':>8}  flag"
    )
    print(header)
    print("-" * 88)
    for row in rows:
        print(
            f"{row['table_name']:<20} {row['check_name']:<36} "
            f"{row['total_rows']:>8,} {row['failed_rows']:>8,} "
            f"{row['pass_pct']:>7.2f}%  {row['flag_token']}"
        )
    print("-" * 88)
    print("Expected failed_rows = 0 (generator enforces the identity).")
    print("Rows are FLAGGED only — nothing was dropped.")
    print("=" * 88)

    summary_df = spark.createDataFrame(
        [
            (
                r["table_name"],
                r["check_name"],
                r["total_rows"],
                r["failed_rows"],
                r["pass_pct"],
                r["flag_token"],
            )
            for r in rows
        ],
        schema=SUMMARY_SCHEMA,
    )
    summary_df.show(truncate=False)
    return summary_df


def _write_silver(df: DataFrame, target_table: str) -> None:
    spark = df.sparkSession
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def run_type_validation_checks(
    spark: SparkSession,
    orders_source: str = DEFAULT_ORDERS_SOURCE,
    orders_target: str = DEFAULT_ORDERS_TARGET,
) -> DataFrame:
    """Flag amount-identity failures, write silver.orders, return summary."""
    print(f"[silver.type] orders {orders_source} → {orders_target}")

    orders_in = spark.table(orders_source)
    rows_before = orders_in.count()
    print(f"[silver.type] orders rows in = {rows_before:,}")

    orders_out = flag_amount_identity(orders_in).persist()
    orders_out.count()

    n_failed = orders_out.filter(
        F.array_contains(F.col("quality_check_result"), FLAG_AMOUNT_MISMATCH)
    ).count()
    pass_pct = (
        round(((rows_before - n_failed) / rows_before) * 100.0, 2) if rows_before else 0.0
    )
    summary_rows = [
        {
            "table_name": orders_target,
            "check_name": "total_amount == quantity * unit_price",
            "total_rows": rows_before,
            "failed_rows": n_failed,
            "pass_pct": pass_pct,
            "flag_token": FLAG_AMOUNT_MISMATCH,
        }
    ]

    _write_silver(orders_out, orders_target)
    orders_out.unpersist()

    rows_after = spark.table(orders_target).count()
    print(f"[silver.type] orders rows out = {rows_after:,}")
    if rows_before != rows_after:
        print(
            "[silver.type] WARNING: row count changed. "
            "Silver must not drop rows — investigate."
        )

    return _print_summary(spark, summary_rows)


if __name__ == "__main__":
    spark = _get_spark()
    run_type_validation_checks(
        spark,
        orders_source=_get_param("orders_source", DEFAULT_ORDERS_SOURCE),
        orders_target=_get_param("orders_target", DEFAULT_ORDERS_TARGET),
    )
