# Data generation notes

Synthetic Bronze-layer source data for the e-commerce medallion pipeline.
No real customer PII — names, emails, and orders are produced by Faker +
`random` with a **fixed seed**.

There is one runner, `src/data_generation/generate_sample_data.py`, with three
generator functions (customers, products, orders). Each function writes one CSV
and prints an injection summary. Defects are applied **after** a clean generate,
on **disjoint** rows, so a reviewer can map each issue to exactly one Silver
check.

---

## What each generator produces

| Function | Output | Rows | Contents |
|---|---|---|---|
| `generate_customers()` + `inject_customer_issues()` | `data/customers.csv` | 10,000 | Customers: PK, name, email, country, signup date, segment, lifetime value |
| `generate_products()` + `inject_product_issues()` | `data/products.csv` | 500 | Products: PK, name, category, price, cost, stock, reorder level |
| `generate_orders()` + `inject_order_issues()` | `data/orders.csv` | 100,000 | Orders: PK, customer FK, product FK, dates, qty, prices, status |

### customers.csv

| Column | How it is generated |
|---|---|
| `customer_id` | Sequential INT PK, 1 .. 10,000 (then 10 IDs are duplicated — see below) |
| `customer_name` | Faker name; locale follows `country` |
| `email` | `{name-slug}{id}@{domain}` — unique and name-aligned |
| `country` | Weighted mix (US 28%, UK 12%, DE 10%, CA 8%, IN 8%, …) |
| `signup_date` | Uniform DATE from `2020-01-01` through `date.today()` |
| `customer_segment` | Premium 20% / Standard 50% / Basic 30% |
| `lifetime_value` | Segment ranges with overlap; **not** summed from orders |

### products.csv

| Column | How it is generated |
|---|---|
| `product_id` | Sequential INT PK, 1 .. 500 |
| `product_name` | Unique `{adjective} {noun}` from an invented catalog |
| `category` | Uniform over 8 values (Electronics, Clothing, Home & Kitchen, Beauty, Sports, Books, Toys, Grocery) |
| `price` | Category-specific USD list-price range |
| `cost` | 40–75% of `price` |
| `stock_quantity` | 1–1500, with ~8% at 0 |
| `reorder_level` | 10–120, independent of current stock |

Products are left **fully clean**. They are the control dimension so order-level
orphan `product_id` values are true missing parents, not a broken product file.

### orders.csv

| Column | How it is generated |
|---|---|
| `order_id` | Sequential INT PK, 1 .. 100,000 (then 20 IDs are duplicated) |
| `customer_id` | Sampled from customer PKs that still exist after customer injection |
| `order_date` | On or after that customer's `signup_date`, never after today |
| `product_id` | Sampled from `products.product_id` |
| `quantity` | 1–8, skewed toward 1–2 |
| `unit_price` | Catalog `price` × [0.90, 1.05] |
| `total_amount` | Always `quantity * unit_price` (2 decimal places) |
| `order_status` | Completed 70% / Pending 18% / Cancelled 12% |
| `payment_date` | Null if Pending; otherwise `order_date` .. today (0–14 day lag) |

---

## Exact quality issues injected

Defects are **in-place mutations** (row counts stay 10,000 / 500 / 100,000).
Each dirty row carries **one** issue so completeness, uniqueness, and
referential-integrity counts stay independently verifiable.

### customers.csv — 60 dirty / 9,940 clean

| Issue | Rows injected | Dimension | Silver script |
|---|---|---|---|
| NULL `email` | **50** | Completeness | `src/silver/01_quality_completeness.py` |
| Duplicate `customer_id` | **10** | Uniqueness | `src/silver/02_quality_uniqueness.py` |

Clean remainder: `10,000 − 50 − 10 = 9,940`.

Duplicate-PK detail: 10 victim rows copy 10 donor PKs. That creates **10**
IDs that appear twice, so a uniqueness check that flags *every* row in a
duplicate group will see **20** rows (victims + donors). The generator
summary prints both numbers.

### products.csv — 0 dirty / 500 clean

No defects. Completeness and uniqueness on products should report zero
failures. Referential integrity uses this file as the product parent.

### orders.csv — 400 dirty / 99,600 clean

| Issue | Rows injected | Dimension | Silver script |
|---|---|---|---|
| NULL `customer_id` | **100** | Completeness | `src/silver/01_quality_completeness.py` |
| NULL `product_id` | **200** | Completeness | `src/silver/01_quality_completeness.py` |
| `customer_id` not in customers | **50** | Referential integrity | `src/silver/04_quality_referential_integrity.py` |
| `product_id` not in products | **30** | Referential integrity | `src/silver/04_quality_referential_integrity.py` |
| Duplicate `order_id` | **20** | Uniqueness | `src/silver/02_quality_uniqueness.py` |

Clean remainder: `100,000 − 100 − 200 − 50 − 30 − 20 = 99,600`.

The brief said “~99,300”; **99,600** is the exact count from the listed
defects (400 dirty rows, disjoint).

Orphan IDs are `max(parent PK) + 1` upward (`customer_id` 10001+,
`product_id` 501+). Duplicate `order_id` behaves like customer duplicates:
20 mutated rows, **40** rows inside duplicate-PK groups.

Null FKs are **completeness** failures, not RI. RI should run on
**non-null** FKs only.

Type validation and business-logic checks have **no** seeded defects in
this pass.

---

## Why these issues were chosen

Silver must **flag** bad rows in `quality_check_result` and never silently
drop them. The injected defects are the three dimensions that matter at
Bronze → Silver for this schema, in sizes large enough to spot in a count
but small enough that Gold still has ~99.6k usable orders.

### Completeness

Missing required fields are the most common Bronze ingest failure (empty
CSV cells, optional-looking columns, failed source extracts).

| Injected issue | Why this field |
|---|---|
| 50 NULL `customers.email` | Email is a required customer attribute; nulls are obvious in a completeness scan and do not break FKs. |
| 100 NULL `orders.customer_id` | A required FK. Completeness must catch “key is missing”; RI must *not* treat null as an orphan. |
| 200 NULL `orders.product_id` | Same rule on the other FK, larger count so the two completeness totals are distinguishable. |

### Uniqueness

Warehouse grain is one row per `customer_id` / `order_id`. Duplicate PKs
from a double-load or a bad merge must be flagged, not collapsed.

| Injected issue | Why this field |
|---|---|
| 10 duplicate `customer_id` | Small, exact count on the customer PK — enough to prove the uniqueness check, not enough to distort Gold segmentation. |
| 20 duplicate `order_id` | Same test at fact-table grain. Kept smaller than the null-FK counts so uniqueness totals cannot be confused with completeness totals. |

### Referential integrity

Orders are only meaningful if they point at a real customer and a real
product. Orphans are a different failure mode from nulls: the key is
present but has no parent.

| Injected issue | Why this field |
|---|---|
| 50 `customer_id` not in customers | Non-null, out-of-range IDs (`10001+`). RI should flag these; completeness should not (the field is populated). |
| 30 `product_id` not in products | Same pattern on the product side. Products were left clean so these cannot be explained by a missing dimension row. |

The two RI counts (50 vs 30) and the two null-FK counts (100 vs 200) are
intentionally different so a wrong join or a swapped column in Silver is
visible in the flagged-row totals.

---

## How to regenerate (fixed seed)

Dependencies: `faker`, `pandas` (Python 3.9+).

```bash
cd databricks-medallion-pipeline
python3 src/data_generation/generate_sample_data.py
```

`SEED = 42` is hardcoded at the top of the script. Do not change it if you
need the same files. Each stream is offset from that constant:

| Stream | Seed value | Role |
|---|---|---|
| Customers (clean generate) | `SEED` = **42** | Names, emails, countries, segments, LTV |
| Products (clean generate) | `SEED + 1` = **43** | Catalog names, prices, stock |
| Orders (clean generate) | `SEED + 2` = **44** | FKs, dates, qty, status |
| Customer issue injection | `SEED + 10` = **52** | Which 50+10 customer rows are mutated |
| Order issue injection | `SEED + 11` = **53** | Which 400 order rows are mutated |

Independent streams mean changing the order injector does not reshuffle
customer names.

On success the script prints three injection summaries. Verify:

```
customers.csv — NULL email 50, duplicate customer_id 10, clean 9940
products.csv  — no issues, 500 rows
orders.csv    — NULL customer_id 100, NULL product_id 200,
                orphan customer_id 50, orphan product_id 30,
                duplicate order_id 20, clean 99600
```

CSV nulls are **empty fields** (`na_rep=""`), not the text `nan`.

### Reproducibility caveat

`signup_date` / `order_date` / `payment_date` use `date.today()` as the
upper bound. Re-running on a **different calendar day** can change dates
(and therefore which random path is taken after the first date draw).
For a bit-identical CSV, re-run on the same date with `SEED = 42`.

---

## Assumptions to verify when you test

1. `customers.lifetime_value` is not derived from order totals — the two can disagree.
2. Product `category` is an invented 8-value list; the source schema did not specify allowed values.
3. `cost < price` is a business assumption, not a schema rule.
4. Order status weights were not in the brief; Completed is the majority so Gold has revenue to sum.
5. Cancelled orders **do** have a `payment_date` — only Pending is null.
6. `unit_price` is close to, but not always equal to, `products.price`.
7. `order_date >= signup_date` is extra realism, not a schema rule.
8. Same-day `payment_date` is allowed when `order_date` is today (never a future date).
9. Some products may have `stock_quantity < reorder_level` or `stock_quantity = 0` — that is stock state, not a seeded DQ issue.
10. Duplicate-PK *injection* counts are the mutated rows (10 / 20); Silver uniqueness that flags every row in a duplicate group will see 20 / 40.
11. Exact clean order count is **99,600**, not ~99,300.
