"""
Silver — referential integrity.

Flag orders whose non-null customer_id is missing from customers, or whose
non-null product_id is missing from products. Uses a left-anti join.
Never drop rows — append tokens onto `quality_check_result`.

Null FKs are completeness failures (01_quality_completeness.py), not RI.
They are filtered out before the anti-join so they are not double-flagged.

Databricks:
    Defaults read silver.orders / silver.customers and bronze.products
    (products were never dirtied; completeness/uniqueness did not write
    silver.products). Writes silver.orders.

Expected seeded failures (from data generation):
    50 orders with customer_id not in customers  (IDs 10001+)
    30 orders with product_id  not in products   (IDs 501+)
    Distinct offending values: 50 customer_ids, 30 product_ids
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

FLAG_ORPHAN_CUSTOMER_ID = "ri.customer_id_orphan"
FLAG_ORPHAN_PRODUCT_ID = "ri.product_id_orphan"

DEFAULT_ORDERS_SOURCE = "workspace.default.silver_orders"
DEFAULT_CUSTOMERS_SOURCE = "workspace.default.silver_customers"
DEFAULT_PRODUCTS_SOURCE = "workspace.default.bronze_products"
DEFAULT_ORDERS_TARGET = "workspace.default.silver_orders"

ROW_ID_COL = "_ri_row_id"

SUMMARY_SCHEMA = StructType(
    [
        StructField("fk_field", StringType(), False),
        StructField("parent_table", StringType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("orphan_order_rows", IntegerType(), False),
        StructField("pct_orders_orphaned", DoubleType(), False),
        StructField("distinct_offending_ids", IntegerType(), False),
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


def _left_anti_orphans(
    child: DataFrame,
    parent: DataFrame,
    fk_column: str,
    parent_pk: str,
) -> DataFrame:
    """Order rows whose FK has no matching parent PK (left-anti).

    WHY left-anti, not NOT IN / left join + IS NULL
    -----------------------------------------------
    `left_anti` is the relational form of "in child, not in parent". Spark
    can broadcast a small parent key set (500 products, ~10k customers) and
    avoid a wide left join that keeps every order column through the join.
    We project parent keys only.

    Null FKs are excluded first: Spark does not treat NULL = NULL as a
    match, so a left-anti on null customer_id would mark completeness
    failures as orphans too.
    """
    parent_keys = (
        parent.select(F.col(parent_pk).alias(fk_column))
        .where(F.col(fk_column).isNotNull())
        .distinct()
    )
    return (
        child.where(F.col(fk_column).isNotNull())
        .join(parent_keys, on=fk_column, how="left_anti")
        .select(ROW_ID_COL, fk_column)
    )


def flag_order_referential_integrity(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Append RI flags to orders. Returns (flagged_orders, orphan_cust, orphan_prod)."""
    orders = _ensure_flag_column(orders).withColumn(
        ROW_ID_COL, F.monotonically_increasing_id()
    )

    orphan_cust = _left_anti_orphans(orders, customers, "customer_id", "customer_id")
    orphan_prod = _left_anti_orphans(orders, products, "product_id", "product_id")

    cust_flag_rows = orphan_cust.select(ROW_ID_COL).withColumn(
        "_orphan_customer", F.lit(True)
    )
    prod_flag_rows = orphan_prod.select(ROW_ID_COL).withColumn(
        "_orphan_product", F.lit(True)
    )

    flagged = (
        orders.join(cust_flag_rows, on=ROW_ID_COL, how="left")
        .join(prod_flag_rows, on=ROW_ID_COL, how="left")
    )

    customer_flags = F.when(
        F.col("_orphan_customer").isNotNull(),
        F.array(F.lit(FLAG_ORPHAN_CUSTOMER_ID)),
    ).otherwise(_empty_flags())

    product_flags = F.when(
        F.col("_orphan_product").isNotNull(),
        F.array(F.lit(FLAG_ORPHAN_PRODUCT_ID)),
    ).otherwise(_empty_flags())

    flagged = (
        flagged.withColumn(
            "quality_check_result",
            F.concat(
                F.coalesce(F.col("quality_check_result"), _empty_flags()),
                customer_flags,
                product_flags,
            ),
        )
        .drop(ROW_ID_COL, "_orphan_customer", "_orphan_product")
    )
    return flagged, orphan_cust, orphan_prod


def _ri_metrics(
    total_orders: int,
    orphan_df: DataFrame,
    fk_field: str,
    parent_table: str,
    flag_token: str,
) -> dict:
    n_orphan_rows = orphan_df.count()
    n_offending_ids = (
        orphan_df.select(fk_field).distinct().count() if n_orphan_rows else 0
    )
    pct = round((n_orphan_rows / total_orders) * 100.0, 4) if total_orders else 0.0
    return {
        "fk_field": fk_field,
        "parent_table": parent_table,
        "total_orders": total_orders,
        "orphan_order_rows": n_orphan_rows,
        "pct_orders_orphaned": pct,
        "distinct_offending_ids": n_offending_ids,
        "flag_token": flag_token,
    }


def _print_summary(spark: SparkSession, rows: list[dict]) -> DataFrame:
    print()
    print("=" * 96)
    print("Silver referential-integrity summary")
    print("=" * 96)
    header = (
        f"{'fk':<14} {'parent':<20} {'orders':>8} {'orphans':>8} "
        f"{'% orphaned':>12} {'distinct ids':>14}  flag"
    )
    print(header)
    print("-" * 96)
    for row in rows:
        print(
            f"{row['fk_field']:<14} {row['parent_table']:<20} "
            f"{row['total_orders']:>8,} {row['orphan_order_rows']:>8,} "
            f"{row['pct_orders_orphaned']:>11.4f}% "
            f"{row['distinct_offending_ids']:>14,}  {row['flag_token']}"
        )
    print("-" * 96)
    print("Null FKs are excluded (completeness, not RI).")
    print("Expected: 50 orphan customer_id rows (50 distinct), 30 orphan product_id rows (30 distinct).")
    print("Rows are FLAGGED only — nothing was dropped.")
    print("=" * 96)

    summary_df = spark.createDataFrame(
        [
            (
                r["fk_field"],
                r["parent_table"],
                r["total_orders"],
                r["orphan_order_rows"],
                r["pct_orders_orphaned"],
                r["distinct_offending_ids"],
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


def run_referential_integrity_checks(
    spark: SparkSession,
    orders_source: str = DEFAULT_ORDERS_SOURCE,
    customers_source: str = DEFAULT_CUSTOMERS_SOURCE,
    products_source: str = DEFAULT_PRODUCTS_SOURCE,
    orders_target: str = DEFAULT_ORDERS_TARGET,
) -> DataFrame:
    """Flag orphan FKs, write silver.orders, return RI summary."""
    print(f"[silver.ri] orders    {orders_source} → {orders_target}")
    print(f"[silver.ri] customers {customers_source}")
    print(f"[silver.ri] products  {products_source}")

    orders_in = spark.table(orders_source)
    customers = spark.table(customers_source)
    products = spark.table(products_source)

    rows_before = orders_in.count()
    print(f"[silver.ri] orders rows in = {rows_before:,}")

    orders_out, orphan_cust, orphan_prod = flag_order_referential_integrity(
        orders_in, customers, products
    )

    # Persist before overwrite (silver.orders → silver.orders).
    orders_out = orders_out.persist()
    orders_out.count()

    summary_rows = [
        _ri_metrics(
            rows_before,
            orphan_cust,
            "customer_id",
            customers_source,
            FLAG_ORPHAN_CUSTOMER_ID,
        ),
        _ri_metrics(
            rows_before,
            orphan_prod,
            "product_id",
            products_source,
            FLAG_ORPHAN_PRODUCT_ID,
        ),
    ]

    _write_silver(orders_out, orders_target)
    orders_out.unpersist()

    rows_after = spark.table(orders_target).count()
    print(f"[silver.ri] orders rows out = {rows_after:,}")
    if rows_before != rows_after:
        print(
            "[silver.ri] WARNING: row count changed. "
            "Silver must not drop rows — investigate."
        )

    return _print_summary(spark, summary_rows)


if __name__ == "__main__":
    spark = _get_spark()
    run_referential_integrity_checks(
        spark,
        orders_source=_get_param("orders_source", DEFAULT_ORDERS_SOURCE),
        customers_source=_get_param("customers_source", DEFAULT_CUSTOMERS_SOURCE),
        products_source=_get_param("products_source", DEFAULT_PRODUCTS_SOURCE),
        orders_target=_get_param("orders_target", DEFAULT_ORDERS_TARGET),
    )
