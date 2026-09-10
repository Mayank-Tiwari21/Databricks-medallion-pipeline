"""
Silver — business-logic checks.

Flag customers whose signup_date is after today, and orders whose
order_date is after today. Never drop rows — append tokens onto
`quality_check_result`.

Null dates are not future dates (completeness would own a missing date).

Databricks:
    Defaults read/write silver.customers and silver.orders
    (run 01–04 first so earlier tokens are preserved).

Expected seeded failures: 0 — the generator caps both dates at date.today()
on the machine that built the CSVs. A cluster whose clock is behind that
generation date could still flag rows; check current_date() if you see hits.
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

FLAG_SIGNUP_IN_FUTURE = "business.signup_date_in_future"
FLAG_ORDER_IN_FUTURE = "business.order_date_in_future"

DEFAULT_CUSTOMERS_SOURCE = "workspace.default.silver_customers"
DEFAULT_ORDERS_SOURCE = "workspace.default.silver_orders"
DEFAULT_CUSTOMERS_TARGET = "workspace.default.silver_customers"
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


def _is_future_date(column: str) -> F.Column:
    """Non-null date strictly after the cluster's current_date()."""
    return F.col(column).isNotNull() & (F.col(column) > F.current_date())


def _append_flag(df: DataFrame, condition: F.Column, token: str) -> DataFrame:
    df = _ensure_flag_column(df)
    new_flags = F.when(condition, F.array(F.lit(token))).otherwise(_empty_flags())
    return df.withColumn(
        "quality_check_result",
        F.concat(
            F.coalesce(F.col("quality_check_result"), _empty_flags()),
            new_flags,
        ),
    )


def flag_customer_business_rules(customers: DataFrame) -> DataFrame:
    """Append business.signup_date_in_future. All rows kept."""
    return _append_flag(customers, _is_future_date("signup_date"), FLAG_SIGNUP_IN_FUTURE)


def flag_order_business_rules(orders: DataFrame) -> DataFrame:
    """Append business.order_date_in_future. All rows kept."""
    return _append_flag(orders, _is_future_date("order_date"), FLAG_ORDER_IN_FUTURE)


def _metric(
    df: DataFrame,
    table_name: str,
    check_name: str,
    flag_token: str,
    total_rows: int,
) -> dict:
    n_failed = df.filter(
        F.array_contains(F.col("quality_check_result"), flag_token)
    ).count()
    pass_pct = (
        round(((total_rows - n_failed) / total_rows) * 100.0, 2) if total_rows else 0.0
    )
    return {
        "table_name": table_name,
        "check_name": check_name,
        "total_rows": total_rows,
        "failed_rows": n_failed,
        "pass_pct": pass_pct,
        "flag_token": flag_token,
    }


def _print_summary(spark: SparkSession, rows: list[dict]) -> DataFrame:
    print()
    print("=" * 88)
    print("Silver business-logic summary")
    print("=" * 88)
    header = (
        f"{'table':<20} {'check':<32} {'rows':>8} {'failed':>8} "
        f"{'pass %':>8}  flag"
    )
    print(header)
    print("-" * 88)
    for row in rows:
        print(
            f"{row['table_name']:<20} {row['check_name']:<32} "
            f"{row['total_rows']:>8,} {row['failed_rows']:>8,} "
            f"{row['pass_pct']:>7.2f}%  {row['flag_token']}"
        )
    print("-" * 88)
    print("Expected failed_rows = 0 (generator caps dates at today).")
    print("Cluster current_date() used as the cutoff.")
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


def run_business_logic_checks(
    spark: SparkSession,
    customers_source: str = DEFAULT_CUSTOMERS_SOURCE,
    orders_source: str = DEFAULT_ORDERS_SOURCE,
    customers_target: str = DEFAULT_CUSTOMERS_TARGET,
    orders_target: str = DEFAULT_ORDERS_TARGET,
) -> DataFrame:
    """Flag future dates, write Silver tables, return summary."""
    print(f"[silver.business] customers {customers_source} → {customers_target}")
    print(f"[silver.business] orders    {orders_source} → {orders_target}")

    customers_in = spark.table(customers_source)
    orders_in = spark.table(orders_source)

    rows_c_before = customers_in.count()
    rows_o_before = orders_in.count()
    print(f"[silver.business] customers rows in = {rows_c_before:,}")
    print(f"[silver.business] orders    rows in = {rows_o_before:,}")

    customers_out = flag_customer_business_rules(customers_in).persist()
    orders_out = flag_order_business_rules(orders_in).persist()
    customers_out.count()
    orders_out.count()

    summary_rows = [
        _metric(
            customers_out,
            customers_target,
            "signup_date <= today",
            FLAG_SIGNUP_IN_FUTURE,
            rows_c_before,
        ),
        _metric(
            orders_out,
            orders_target,
            "order_date <= today",
            FLAG_ORDER_IN_FUTURE,
            rows_o_before,
        ),
    ]

    _write_silver(customers_out, customers_target)
    _write_silver(orders_out, orders_target)
    customers_out.unpersist()
    orders_out.unpersist()

    rows_c_after = spark.table(customers_target).count()
    rows_o_after = spark.table(orders_target).count()
    print(f"[silver.business] customers rows out = {rows_c_after:,}")
    print(f"[silver.business] orders    rows out = {rows_o_after:,}")
    if rows_c_before != rows_c_after or rows_o_before != rows_o_after:
        print(
            "[silver.business] WARNING: row count changed. "
            "Silver must not drop rows — investigate."
        )

    return _print_summary(spark, summary_rows)


if __name__ == "__main__":
    spark = _get_spark()
    run_business_logic_checks(
        spark,
        customers_source=_get_param("customers_source", DEFAULT_CUSTOMERS_SOURCE),
        orders_source=_get_param("orders_source", DEFAULT_ORDERS_SOURCE),
        customers_target=_get_param("customers_target", DEFAULT_CUSTOMERS_TARGET),
        orders_target=_get_param("orders_target", DEFAULT_ORDERS_TARGET),
    )
