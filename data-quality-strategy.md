# Data quality strategy

Silver-layer rule: **flag** bad rows in `quality_check_result`. Never silently drop rows.

## Checks to implement

1. Completeness
2. Uniqueness
3. Type validation
4. Referential integrity
5. Business logic

## Seeded defects in Bronze CSVs

These are planted by `src/data_generation/generate_sample_data.py` so each
Silver script has a known number of failures to catch. One issue per dirty
row. Products are left clean.

| Silver script | What to flag | Expected rows |
|---|---|---|
| `01_quality_completeness.py` | customers.email is null | 50 |
| `01_quality_completeness.py` | orders.customer_id is null | 100 |
| `01_quality_completeness.py` | orders.product_id is null | 200 |
| `02_quality_uniqueness.py` | duplicate customers.customer_id | 10 mutated (20 rows in dup groups) |
| `02_quality_uniqueness.py` | duplicate orders.order_id | 20 mutated (40 rows in dup groups) |
| `04_quality_referential_integrity.py` | orders.customer_id not in customers | 50 |
| `04_quality_referential_integrity.py` | orders.product_id not in products | 30 |

Null FKs are completeness failures, not RI failures. RI should run on
**non-null** FKs only.

Type validation and business logic have **no** seeded defects in this pass.
`03_quality_type_validation.py` and `05_quality_business_logic.py` still run
so a bad re-ingest (or an inferSchema double) would be flagged. Expected
failure counts are 0.

## `quality_check_result` tokens (completeness)

Column type: `array<string>`. Empty array = row passed completeness.
Tokens are concatenated by later Silver scripts (never overwritten).

| Token | Field | Expected rows |
|---|---|---|
| `completeness.email_is_null` | customers.email | 50 |
| `completeness.customer_id_is_null` | orders.customer_id | 100 |
| `completeness.product_id_is_null` | orders.product_id | 200 |

## `quality_check_result` tokens (uniqueness)

All rows in a duplicate PK group are flagged (victim **and** donor).

| Token | Field | Expected rows |
|---|---|---|
| `uniqueness.customer_id_duplicate` | customers.customer_id | 20 (10 IDs × 2) |
| `uniqueness.order_id_duplicate` | orders.order_id | 40 (20 IDs × 2) |

`pct_unique_ids` = `distinct(pk) / row_count` (~99.90% customers, ~99.98% orders).

## `quality_check_result` tokens (referential integrity)

Null FKs are **not** RI failures. Left-anti runs on non-null keys only.

| Token | Field | Expected rows | Distinct offending IDs |
|---|---|---|---|
| `ri.customer_id_orphan` | orders.customer_id | 50 | 50 |
| `ri.product_id_orphan` | orders.product_id | 30 | 30 |

`pct_orders_orphaned` = orphan rows / total orders (~0.05% and ~0.03%).

## `quality_check_result` tokens (type validation)

No seeded defects. Generator sets `total_amount = round(quantity * unit_price, 2)`.

| Token | Rule | Expected rows |
|---|---|---|
| `type.total_amount_neq_qty_times_price` | `round(quantity * unit_price, 2) != total_amount` | 0 |

## `quality_check_result` tokens (business logic)

No seeded defects. Generator caps dates at `date.today()`.

| Token | Rule | Expected rows |
|---|---|---|
| `business.signup_date_in_future` | `signup_date > current_date()` | 0 |
| `business.order_date_in_future` | `order_date > current_date()` | 0 |

## Orchestrator

`src/silver/create_silver_tables.py` reads Bronze, applies every flag function
in memory (one merged `quality_check_result` per row), and writes:

| Table | Contents |
|---|---|
| `silver.customers` | All 10,000 rows + flags |
| `silver.orders` | All 100,000 rows + flags |
| `silver.products` | All 500 rows + empty flag array |
| `silver.quality_metrics` | One row per (table, check) with `pass_pct` |

```sql
SELECT * FROM silver.quality_metrics ORDER BY table_name, check_group;
-- Gold grain: rows that passed every check
SELECT * FROM silver.orders WHERE size(quality_check_result) = 0;
```

Expected overall pass (disjoint seeded defects):

- customers: `10000 - 50 - 20 = 9930` → 99.30%
- orders: `100000 - 100 - 200 - 40 - 50 - 30 = 99580` → 99.58%
- products: `500 / 500` → 100%

## How Gold uses these flags

Gold never drops Silver rows itself; it **filters**:

```sql
WHERE size(quality_check_result) = 0
```

on every Silver table it reads. A customer with `uniqueness.customer_id_duplicate`
is excluded entirely (both copies). Their orders still exist in Silver; they
only enter Gold if the **order** row is clean and the join parent is clean.

Cancelled is a **business filter in Gold**, not a Silver quality token.
Pending orders stay in Gold revenue.

## Running the checks

`src/run_pipeline.py` runs all five Silver scripts via
`create_silver_tables.py` after Bronze. On Databricks Free Edition that is the
supported path (no DBFS; notebooks have no `__file__`).

```sql
SELECT table_name, check_name, failed_rows, pass_pct
FROM silver.quality_metrics
ORDER BY table_name, check_group, check_name;
```

If `failed_rows` for completeness / uniqueness / RI do not match the table
above, the generator and Silver have drifted — re-run
`src/data_generation/generate_sample_data.py` (seeded) and re-ingest.

## Assumptions

- One issue per dirty source row (disjoint injection).
- Uniqueness flags **all** members of a duplicate PK group.
- Type and business checks are live tripwires with expected 0 failures.
- `current_date()` for business-date checks is the **cluster** date, not the
  laptop date used when CSVs were generated.
