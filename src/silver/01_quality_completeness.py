"""
Silver — completeness checks.

Flag rows with NULL (or blank) customers.email, orders.customer_id, or
orders.product_id. Never drop rows. Failures are appended to
`quality_check_result` (array<string>).

Databricks:
    Defaults read bronze.* and write silver.*. Override with widgets
    `customers_source`, `orders_source`, `customers_target`, `orders_target`.

Expected seeded failures (from data generation):
    customers.email         50 nulls  → completeness ~99.50%
    orders.customer_id     100 nulls  → completeness ~99.90%
    orders.product_id      200 nulls  → completeness ~99.80%
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

# Tokens stored in quality_check_result so later Silver scripts can append
# uniqueness / RI / business-logic codes without colliding.
FLAG_EMAIL_NULL = "completeness.email_is_null"
FLAG_CUSTOMER_ID_NULL = "completeness.customer_id_is_null"
FLAG_PRODUCT_ID_NULL = "completeness.product_id_is_null"

DEFAULT_CUSTOMERS_SOURCE = "bronze.customers"
DEFAULT_ORDERS_SOURCE = "bronze.orders"
DEFAULT_CUSTOMERS_TARGET = "silver.customers"
DEFAULT_ORDERS_TARGET = "silver.orders"

SUMMARY_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("field_name", StringType(), False),
        StructField("total_rows", IntegerType(), False),
        StructField("null_or_blank_rows", IntegerType(), False),
        StructField("completeness_pct", DoubleType(), False),
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


def _is_missing(column: str) -> F.Column:
    """NULL or whitespace-only (covers STRING emails and CSV leftovers)."""
    as_str = F.trim(F.col(column).cast("string"))
    return F.col(column).isNull() | (as_str == "")


def _empty_flags() -> F.Column:
    return F.array().cast("array<string>")


def _flag_if_missing(column: str, token: str) -> F.Column:
    """array(token) when the field is missing, else empty array."""
    return F.when(_is_missing(column), F.array(F.lit(token))).otherwise(
        _empty_flags()
    )


def flag_customer_completeness(customers: DataFrame) -> DataFrame:
    """Add quality_check_result for NULL/blank email. All rows kept."""
    return customers.withColumn(
        "quality_check_result",
        _flag_if_missing("email", FLAG_EMAIL_NULL),
    )


def flag_order_completeness(orders: DataFrame) -> DataFrame:
    """Add quality_check_result for NULL customer_id and/or product_id.

    Concatenate the two flag arrays so a row that failed both checks
    records both tokens. (The generator plants them on disjoint rows.)
    All rows kept.
    """
    customer_flags = _flag_if_missing("customer_id", FLAG_CUSTOMER_ID_NULL)
    product_flags = _flag_if_missing("product_id", FLAG_PRODUCT_ID_NULL)
    return orders.withColumn(
        "quality_check_result",
        F.concat(customer_flags, product_flags),
    )


def _field_completeness(
    df: DataFrame, table_name: str, field_name: str, flag_token: str
) -> dict:
    total = df.count()
    n_missing = df.filter(_is_missing(field_name)).count()
    complete = total - n_missing
    pct = round((complete / total) * 100.0, 2) if total else 0.0
    return {
        "table_name": table_name,
        "field_name": field_name,
        "total_rows": total,
        "null_or_blank_rows": n_missing,
        "completeness_pct": pct,
        "flag_token": flag_token,
    }


def _print_summary(spark: SparkSession, rows: list[dict]) -> DataFrame:
    print()
    print("=" * 78)
    print("Silver completeness summary")
    print("=" * 78)
    header = (
        f"{'table':<20} {'field':<14} {'total':>8} {'null/blank':>12} "
        f"{'completeness':>14}  flag"
    )
    print(header)
    print("-" * 78)
    for row in rows:
        print(
            f"{row['table_name']:<20} {row['field_name']:<14} "
            f"{row['total_rows']:>8,} {row['null_or_blank_rows']:>12,} "
            f"{row['completeness_pct']:>13.2f}%  {row['flag_token']}"
        )
    print("-" * 78)
    print("Rows are FLAGGED only — nothing was dropped.")
    print("Expected null/blank: email 50, customer_id 100, product_id 200.")
    print("=" * 78)

    summary_df = spark.createDataFrame(
        [
            (
                r["table_name"],
                r["field_name"],
                r["total_rows"],
                r["null_or_blank_rows"],
                r["completeness_pct"],
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
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def run_completeness_checks(
    spark: SparkSession,
    customers_source: str = DEFAULT_CUSTOMERS_SOURCE,
    orders_source: str = DEFAULT_ORDERS_SOURCE,
    customers_target: str = DEFAULT_CUSTOMERS_TARGET,
    orders_target: str = DEFAULT_ORDERS_TARGET,
) -> DataFrame:
    """Flag completeness failures, write Silver tables, return summary DF."""
    print(f"[silver.completeness] customers {customers_source} → {customers_target}")
    print(f"[silver.completeness] orders    {orders_source} → {orders_target}")

    customers_in = spark.table(customers_source)
    orders_in = spark.table(orders_source)

    rows_c_before = customers_in.count()
    rows_o_before = orders_in.count()
    print(f"[silver.completeness] customers rows in  = {rows_c_before:,}")
    print(f"[silver.completeness] orders    rows in  = {rows_o_before:,}")

    customers_out = flag_customer_completeness(customers_in)
    orders_out = flag_order_completeness(orders_in)

    _write_silver(customers_out, customers_target)
    _write_silver(orders_out, orders_target)

    rows_c_after = spark.table(customers_target).count()
    rows_o_after = spark.table(orders_target).count()
    print(f"[silver.completeness] customers rows out = {rows_c_after:,}")
    print(f"[silver.completeness] orders    rows out = {rows_o_after:,}")
    if rows_c_before != rows_c_after or rows_o_before != rows_o_after:
        print(
            "[silver.completeness] WARNING: row count changed. "
            "Silver must not drop rows — investigate."
        )

    summary_rows = [
        _field_completeness(
            customers_out, customers_target, "email", FLAG_EMAIL_NULL
        ),
        _field_completeness(
            orders_out, orders_target, "customer_id", FLAG_CUSTOMER_ID_NULL
        ),
        _field_completeness(
            orders_out, orders_target, "product_id", FLAG_PRODUCT_ID_NULL
        ),
    ]
    return _print_summary(spark, summary_rows)


if __name__ == "__main__":
    spark = _get_spark()
    run_completeness_checks(
        spark,
        customers_source=_get_param("customers_source", DEFAULT_CUSTOMERS_SOURCE),
        orders_source=_get_param("orders_source", DEFAULT_ORDERS_SOURCE),
        customers_target=_get_param("customers_target", DEFAULT_CUSTOMERS_TARGET),
        orders_target=_get_param("orders_target", DEFAULT_ORDERS_TARGET),
    )
