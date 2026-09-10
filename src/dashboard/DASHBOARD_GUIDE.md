# Dashboard guide

How to turn `src/dashboard/dashboard_queries.sql` into a Databricks SQL
dashboard with three charts and optional filters.

**Prerequisite:** Gold tables exist (`gold.sales_by_product`,
`gold.revenue_by_customer`, `gold.customer_segmentation`). Prefer
`src/run_pipeline.py`, which materializes Gold and then `display()`s these
three queries. You can also run `src/gold/create_gold_tables.py` alone.

**Community Edition:** SQL Warehouses and SQL Dashboards are often **not**
available. Use the [notebook fallback](#community-edition-notebook-fallback)
at the bottom. The field mappings are the same.

---

## 0. Create three saved queries

Do **not** paste the whole `.sql` file into one editor. Databricks SQL runs
the last statement (or errors on multiple). One query per widget.

1. Open **SQL Editor** (sidebar → SQL).
2. Attach a **SQL warehouse** and confirm `USE hive_metastore` (or your catalog).
3. For each block below, **New query** → paste **only that SELECT** → **Run** →
   **Save** with the name given.

| Saved query name | Block in `dashboard_queries.sql` |
|---|---|
| Top 10 products by revenue | Query (1) |
| Customer revenue distribution | Query (2) |
| Customer segment mix | Query (3) |

---

## 1. Bar chart — Top 10 products by revenue

**Saved query:** Top 10 products by revenue  
**Chart type:** Bar

1. On the query results, click **+** → **Visualization** (or the chart icon).
2. **Visualization type:** Bar.
3. Map fields:

| Control | Field |
|---|---|
| X axis | `product_name` |
| Y axis | `revenue_usd` |
| Group / color (optional) | `category` |
| Sort | Leave the SQL `ORDER BY total_revenue DESC` — do not re-sort A–Z on the name |

4. Y-axis formatting: **Currency** (USD), 0–2 decimal places.
5. Name the viz **Top 10 products by revenue**. Save.

X is the product name (readable label). Y is `revenue_usd`, not `order_count`.
`order_count` is extra if you add a tooltip.

---

## 2. Histogram — customer revenue distribution

Databricks SQL has **no histogram chart**. Use a **bar** of pre-bucketed counts
(query 2 already builds `revenue_bucket`).

**Saved query:** Customer revenue distribution  
**Chart type:** Bar

1. Add a visualization → **Bar**.
2. Map fields:

| Control | Field |
|---|---|
| X axis | `revenue_bucket` |
| Y axis | `customer_count` |
| Sort | `sort_order` **ascending** (not alphabetical — otherwise `$1,000` sorts before `$0`) |

3. X-axis: hide `sort_order` if it appears as a series; it is only for order.
4. Name the viz **Customer revenue distribution**. Save.

If one bar dominates, edit the `CASE` cuts in query 2 and re-run.

---

## 3. Pie chart — customer segment mix

**Saved query:** Customer segment mix  
**Chart type:** Pie

This is **behavioral** `segment_type` (High-Value / Repeat / One-Time /
Inactive), **not** Premium / Standard / Basic.

1. Add a visualization → **Pie**.
2. Map fields:

| Control | Field |
|---|---|
| Label / group | `segment_type` |
| Value / angle | `customer_count` |

3. To show **revenue** instead of headcount, set Value to `revenue_usd`
   (duplicate the viz if you want both pies).
4. Name the viz **Customers by behavioral segment**. Save.

---

## 4. Assemble the dashboard

### Classic SQL Dashboard (legacy)

1. **Dashboards** → **Create dashboard**.
2. **Add** → pick each saved visualization (bar, histogram-bar, pie).
3. Arrange: products bar on top, histogram middle, pie on the side.
4. **Save**. Refresh uses the warehouse; it does not rebuild Gold.

### Lakeview / AI/BI Dashboard (newer workspaces)

1. **Dashboards** → **Create**.
2. **Add data** → choose the three saved queries (or paste them as datasets).
3. **Add visualization** on each dataset with the same field mappings as above.
4. **Publish**.

---

## 5. Filters

### Segment filter (works today)

Gold already has two different “segment” fields. Use the one that matches the
chart.

**A. Source-system segment (Premium / Standard / Basic)** — histogram only  
`gold.revenue_by_customer.customer_segment`

1. Edit **Customer revenue distribution**.
2. Add a query parameter. In Databricks SQL the placeholder is `{{ ... }}`.

```sql
FROM gold.revenue_by_customer
WHERE (
    :include_all_segments = true
    OR customer_segment IN ({{ customer_segment }})
)
```

Simpler dropdown (single value):

```sql
FROM gold.revenue_by_customer
WHERE customer_segment = {{ customer_segment }}
```

3. In the query **Parameters** panel:
   - Name: `customer_segment`
   - Type: **Dropdown List**
   - Values: `Premium`, `Standard`, `Basic`  
     (or **Query Based Dropdown** →
     `SELECT DISTINCT customer_segment FROM gold.revenue_by_customer`)
4. On the **dashboard**, **Add filter** → map it to parameter
   `customer_segment` so one control drives the histogram.

Query 1 (products) has **no** customer segment — a dashboard-level customer
filter will not change that bar unless you rebuild `gold.sales_by_product`
with a segment grain.

**B. Behavioral segment (High-Value / Repeat / One-Time / Inactive)** — pie  
Filtering the pie by `segment_type` just hides slices. Prefer using the pie
as the **overview**, and filter the histogram with Premium/Standard/Basic
instead.

If you still want it:

```sql
FROM gold.customer_segmentation
WHERE segment_type = {{ segment_type }}
```

Parameter type: Dropdown with those four labels.

### Date-range filter (limitation + how to wire it)

The three Gold tables are **already aggregated**. They have **no**
`order_date`, so a date picker **cannot** slice them as they stand.

| Gold table | Date column? |
|---|---|
| `gold.sales_by_product` | No |
| `gold.revenue_by_customer` | No |
| `gold.customer_segmentation` | No |

`03_daily_weekly_trends.sql` is the right grain for a date filter; it is not
built yet.

**When that table exists**, add dashboard parameters and a WHERE clause:

```sql
-- Example only — requires a Gold table with order_date or order_week
WHERE order_date BETWEEN {{ start_date }} AND {{ end_date }}
```

1. Query **Parameters**:
   - `start_date` — type **Date**, default e.g. `2020-01-01`
   - `end_date` — type **Date**, default today
2. Dashboard → **Add filter** → **Date range** → map **Start** to
   `start_date` and **End** to `end_date`.
3. Use **the same parameter names** on every query that should move together.

**Do not** add `WHERE order_date BETWEEN ...` to the current three queries —
the columns are not there, and the query will fail.

Until daily Gold exists, use the **segment** filter above, or rebuild Gold
inside a date window from Silver (that would no longer be “Gold tables only”).

---

## Field mapping cheat sheet

| Viz | Type | X / Label | Y / Value | Filter you can add |
|---|---|---|---|---|
| Top 10 products | Bar | `product_name` | `revenue_usd` | none on current Gold |
| Revenue distribution | Bar | `revenue_bucket` (sort `sort_order`) | `customer_count` | `customer_segment` |
| Segment mix | Pie | `segment_type` | `customer_count` or `revenue_usd` | `segment_type` (optional) |

---

## Community Edition notebook fallback

`src/run_pipeline.py` already `display()`s these three queries after Gold.
Use the steps below to turn them into a SQL Dashboard (paid workspaces) or
to re-chart them in extra notebook cells.

1. In a notebook attached to the cluster:

```python
display(spark.sql("""
-- paste query 1 here
"""))
```

2. In the result table, click the chart icon.
3. Pick **Bar** / **Pie** and the same field mappings as above.
4. Repeat for queries 2 and 3 in new cells.

There is no dashboard-level date parameter in this path; filters are `WHERE`
clauses in the cell.

---

## Assumptions

1. SQL Dashboard UI labels (X axis vs “Category”) vary slightly by workspace
   (classic vs Lakeview). The **column names** above are what matter.
2. Date-range filtering is documented for when daily Gold exists; it does
   not apply to the three queries in `dashboard_queries.sql` today.
3. Histogram sort must use `sort_order`, not the bucket label string.
4. Refreshing the dashboard does not re-run Bronze/Silver/Gold pipelines.
