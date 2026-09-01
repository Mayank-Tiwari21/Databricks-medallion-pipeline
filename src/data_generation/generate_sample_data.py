"""
Generate synthetic e-commerce source data for the Bronze layer.

Output
------
data/customers.csv  — 10,000 rows  (seeded completeness + uniqueness defects)
data/products.csv   — 500 rows     (left clean — control file for FK checks)
data/orders.csv     — 100,000 rows (seeded completeness, uniqueness, RI defects)

All values are synthetic (Faker + random). No real customer PII is used.

Seeded quality issues are injected AFTER a clean generate, on disjoint rows,
so a reviewer can map each defect type to exactly one Silver check. See
inject_customer_issues / inject_product_issues / inject_order_issues.

Foreign keys
------------
orders.customer_id  →  customers.customer_id
orders.product_id   →  products.product_id

Usage
-----
    python3 src/data_generation/generate_sample_data.py

Re-running with the same SEED produces the same files.
"""

from __future__ import annotations

import random
import unicodedata
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
# Customers use SEED, products use SEED+1, orders use SEED+2 so each file has
# an independent stream. Changing one generator does not reshuffle the others.
SEED = 42
N_CUSTOMERS = 10_000
N_PRODUCTS = 500
N_ORDERS = 100_000

# Date window shared by signup_date and order_date.
SIGNUP_START = date(2020, 1, 1)
TODAY = date.today()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
CUSTOMERS_PATH = DATA_DIR / "customers.csv"
PRODUCTS_PATH = DATA_DIR / "products.csv"
ORDERS_PATH = DATA_DIR / "orders.csv"

# ---------------------------------------------------------------------------
# Country mix (customers)
# ---------------------------------------------------------------------------
COUNTRY_SPEC = [
    # (country_name, weight, faker_locale)
    ("United States", 0.28, "en_US"),
    ("United Kingdom", 0.12, "en_GB"),
    ("Germany", 0.10, "de_DE"),
    ("Canada", 0.08, "en_CA"),
    ("India", 0.08, "en_IN"),
    ("France", 0.07, "fr_FR"),
    ("Australia", 0.06, "en_AU"),
    ("Netherlands", 0.04, "nl_NL"),
    ("Brazil", 0.04, "pt_BR"),
    ("Japan", 0.04, "en_US"),  # en_US: romanized names so emails stay ASCII
    ("Spain", 0.03, "es_ES"),
    ("Italy", 0.03, "it_IT"),
    ("Mexico", 0.03, "es_MX"),
]

COUNTRIES = [c[0] for c in COUNTRY_SPEC]
COUNTRY_WEIGHTS = [c[1] for c in COUNTRY_SPEC]
COUNTRY_LOCALE = {c[0]: c[2] for c in COUNTRY_SPEC}

SEGMENTS = ["Premium", "Standard", "Basic"]
SEGMENT_WEIGHTS = [0.20, 0.50, 0.30]

LTV_RANGES = {
    "Premium": (800.00, 4500.00),
    "Standard": (150.00, 1800.00),
    "Basic": (25.00, 700.00),
}

EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "proton.me",
]

# ---------------------------------------------------------------------------
# Product catalog
# ---------------------------------------------------------------------------
# Categories are not specified in the source schema; these are a typical
# e-commerce mix. Price ranges are USD list-price bounds per category.
# Names are built as "{adjective} {noun}" from invented (not trademarked) brands
# mixed into the adjective list so every name looks like a real SKU.
PRODUCT_CATEGORIES = {
    "Electronics": {
        "nouns": [
            "Wireless Earbuds", "Bluetooth Speaker", "USB-C Hub", "Phone Stand",
            "Laptop Sleeve", "Webcam", "HDMI Cable", "Power Bank",
            "Smart Watch Band", "Mechanical Keyboard", "Wireless Mouse",
            "Monitor Light Bar", "USB Flash Drive", "Laptop Charger",
        ],
        "price_range": (12.99, 399.99),
    },
    "Clothing": {
        "nouns": [
            "Cotton T-Shirt", "Denim Jacket", "Running Shorts", "Wool Beanie",
            "Crew Socks", "Hoodie", "Chino Pants", "Rain Jacket",
            "Linen Shirt", "Knit Sweater", "Baseball Cap", "Yoga Leggings",
        ],
        "price_range": (9.99, 149.99),
    },
    "Home & Kitchen": {
        "nouns": [
            "Ceramic Mug", "Nonstick Pan", "Cutting Board", "Kitchen Scale",
            "Throw Pillow", "LED Desk Lamp", "Storage Bin", "Coffee Grinder",
            "Water Bottle", "Towel Set", "Stainless Mixing Bowl", "Oven Mitt",
        ],
        "price_range": (7.99, 129.99),
    },
    "Beauty": {
        "nouns": [
            "Moisturizer", "Lip Balm", "Hair Brush", "Face Serum",
            "Body Lotion", "Nail Kit", "Makeup Mirror", "Shower Gel",
        ],
        "price_range": (4.99, 79.99),
    },
    "Sports": {
        "nouns": [
            "Yoga Mat", "Resistance Band", "Jump Rope", "Dumbbell Set",
            "Cycling Gloves", "Gym Bag", "Foam Roller", "Tennis Ball Pack",
        ],
        "price_range": (8.99, 199.99),
    },
    "Books": {
        "nouns": [
            "Hardcover Notebook", "Desk Planner", "Sketch Pad",
            "Cookbook Stand", "Reading Light", "Book Ends",
        ],
        "price_range": (6.99, 39.99),
    },
    "Toys": {
        "nouns": [
            "Building Blocks", "Puzzle Set", "Plush Animal", "RC Car",
            "Board Game", "Art Kit", "Yo-Yo", "Stuffed Robot",
        ],
        "price_range": (5.99, 89.99),
    },
    "Grocery": {
        "nouns": [
            "Organic Coffee Beans", "Green Tea Box", "Granola Mix",
            "Olive Oil Bottle", "Dark Chocolate Bar", "Spice Set",
            "Trail Mix Pack", "Honey Jar",
        ],
        "price_range": (3.99, 34.99),
    },
}

PRODUCT_ADJECTIVES = [
    "Apex", "Nimbus", "Harbor", "Lumen", "Northstar", "Cedar", "Summit",
    "Orbit", "Willow", "Cascade", "Classic", "Deluxe", "Compact", "Everyday",
    "Trail", "Urban", "Pro", "Lite", "Studio", "Heritage",
]

# ---------------------------------------------------------------------------
# Order status mix (not specified in the brief — see assumptions)
# ---------------------------------------------------------------------------
# Completed is the majority so Gold revenue aggregations have something to sum.
# Pending has no payment_date. Cancelled still gets a payment_date because the
# brief says "null when Pending, populated otherwise".
ORDER_STATUSES = ["Completed", "Pending", "Cancelled"]
ORDER_STATUS_WEIGHTS = [0.70, 0.18, 0.12]

# ---------------------------------------------------------------------------
# Seeded data-quality defects (Prompt 3)
# ---------------------------------------------------------------------------
# Counts are the number of rows *mutated*. Injections are disjoint so a row
# carries at most one issue. Remaining rows stay clean:
#   customers: 10,000 - 50 - 10 = 9,940
#   orders:    100,000 - 100 - 200 - 50 - 30 - 20 = 99,600
#              (brief said ~99,300; 99,600 is the exact remainder)
N_NULL_EMAIL = 50
N_DUP_CUSTOMER_ID = 10
N_NULL_ORDER_CUSTOMER_ID = 100
N_NULL_ORDER_PRODUCT_ID = 200
N_ORPHAN_CUSTOMER_ID = 50
N_ORPHAN_PRODUCT_ID = 30
N_DUP_ORDER_ID = 20


def _ascii_slug(text: str) -> str:
    """Fold accented characters to ASCII and keep letters/digits only."""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = "".join(ch.lower() if ch.isalnum() else "." for ch in ascii_text)
    parts = [p for p in cleaned.split(".") if p]
    return ".".join(parts)


def _email_from_name(name: str, customer_id: int, rng: random.Random) -> str:
    """Build a unique, name-based email: jane.doe142@gmail.com."""
    slug = _ascii_slug(name)
    if not slug:
        slug = f"customer.{customer_id}"
    domain = rng.choice(EMAIL_DOMAINS)
    return f"{slug}{customer_id}@{domain}"


def _random_date(rng: random.Random, start: date, end: date) -> date:
    """Uniform random date between start and end (inclusive)."""
    if end < start:
        return start
    span_days = (end - start).days
    return start + timedelta(days=rng.randint(0, span_days))


def _lifetime_value(segment: str, rng: random.Random) -> float:
    """Draw LTV from the segment's overlapping range, 2 decimal places."""
    low, high = LTV_RANGES[segment]
    return round(rng.uniform(low, high), 2)


# ===========================================================================
# Customers
# ===========================================================================

def generate_customers(n: int = N_CUSTOMERS, seed: int = SEED) -> pd.DataFrame:
    """Return a DataFrame of `n` synthetic customers."""
    rng = random.Random(seed)

    fakers: dict[str, Faker] = {}
    for locale in set(COUNTRY_LOCALE.values()):
        fake = Faker(locale)
        fake.seed_instance(seed)
        fakers[locale] = fake

    rows: list[dict] = []

    for customer_id in range(1, n + 1):
        # customer_id: sequential integer PK, starting at 1, no gaps.

        # country: weighted choice from COUNTRY_SPEC (not uniform).
        country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]

        # customer_name: Faker name from the locale that matches `country`.
        locale = COUNTRY_LOCALE[country]
        customer_name = fakers[locale].name()

        # email: derived from the name + customer_id so it is unique and
        # visually tied to the person.
        email = _email_from_name(customer_name, customer_id, rng)

        # signup_date: uniform DATE between 2020-01-01 and today.
        signup_date = _random_date(rng, SIGNUP_START, TODAY)

        # customer_segment: Premium ~20%, Standard ~50%, Basic ~30%.
        customer_segment = rng.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]

        # lifetime_value: DECIMAL drawn from a segment-specific range.
        # Independent of orders — not a sum of order totals.
        lifetime_value = _lifetime_value(customer_segment, rng)

        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "country": country,
                "signup_date": signup_date.isoformat(),
                "customer_segment": customer_segment,
                "lifetime_value": lifetime_value,
            }
        )

    return pd.DataFrame(rows)


# ===========================================================================
# Products
# ===========================================================================

def generate_products(n: int = N_PRODUCTS, seed: int = SEED + 1) -> pd.DataFrame:
    """Return a DataFrame of `n` synthetic products."""
    rng = random.Random(seed)
    category_names = list(PRODUCT_CATEGORIES.keys())
    used_names: set[str] = set()
    rows: list[dict] = []

    for product_id in range(1, n + 1):
        # product_id: sequential integer PK, starting at 1, no gaps.

        # category: uniform over the 8 catalog categories.
        category = rng.choice(category_names)
        spec = PRODUCT_CATEGORIES[category]

        # product_name: "{adjective} {noun}". If the combo is already used,
        # append the product_id so the 500 names stay unique.
        adjective = rng.choice(PRODUCT_ADJECTIVES)
        noun = rng.choice(spec["nouns"])
        product_name = f"{adjective} {noun}"
        if product_name in used_names:
            product_name = f"{product_name} #{product_id}"
        used_names.add(product_name)

        # price: list price in USD, 2 decimals, drawn from the category range.
        low, high = spec["price_range"]
        price = round(rng.uniform(low, high), 2)

        # cost: 40–75% of price so every SKU has a positive margin.
        # Schema does not require cost < price; this is a business assumption.
        cost = round(price * rng.uniform(0.40, 0.75), 2)

        # stock_quantity: on-hand units. Most SKUs are in stock; ~8% are at 0.
        if rng.random() < 0.08:
            stock_quantity = 0
        else:
            stock_quantity = rng.randint(1, 1500)

        # reorder_level: trigger for replenishment, typically 10–120 units.
        # Independent of current stock so some rows will be "below reorder"
        # (stock_quantity < reorder_level) — realistic, not a data error.
        reorder_level = rng.randint(10, 120)

        rows.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price": price,
                "cost": cost,
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
            }
        )

    return pd.DataFrame(rows)


# ===========================================================================
# Orders
# ===========================================================================

def generate_orders(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    n: int = N_ORDERS,
    seed: int = SEED + 2,
) -> pd.DataFrame:
    """Return a DataFrame of `n` synthetic orders with valid FKs.

    Call this *after* customer-issue injection, then run inject_order_issues
    to plant the seeded defects. Pre-injection rows reference only IDs that
    still exist in the (possibly-duplicated) customer PK set.
    """
    rng = random.Random(seed)

    # After customer-issue injection, 10 original PKs were overwritten
    # (those IDs now appear twice on other rows). Sample only distinct,
    # non-null IDs so the pre-injection order rows stay referentially clean.
    valid_customers = (
        customers.dropna(subset=["customer_id"])
        .drop_duplicates(subset=["customer_id"], keep="first")
    )
    customer_ids = valid_customers["customer_id"].astype(int).tolist()
    signup_by_id = dict(
        zip(
            valid_customers["customer_id"].astype(int),
            pd.to_datetime(valid_customers["signup_date"]).dt.date,
        )
    )
    product_ids = products["product_id"].astype(int).tolist()
    price_by_id = dict(
        zip(products["product_id"].astype(int), products["price"])
    )

    rows: list[dict] = []

    for order_id in range(1, n + 1):
        # order_id: sequential integer PK, starting at 1, no gaps.

        # customer_id: FK — uniform sample from existing customer PKs.
        customer_id = rng.choice(customer_ids)

        # product_id: FK — uniform sample from existing product PKs.
        product_id = rng.choice(product_ids)

        # order_date: on or after that customer's signup_date, never after today.
        # If they signed up today, the only legal order_date is today.
        signup = signup_by_id[customer_id]
        order_date = _random_date(rng, signup, TODAY)

        # quantity: 1–8 units, skewed toward small baskets (1–2 most common).
        quantity = rng.choices(
            [1, 2, 3, 4, 5, 6, 7, 8],
            weights=[35, 25, 15, 10, 7, 4, 2, 2],
            k=1,
        )[0]

        # unit_price: catalog price at order time, with a small promo/surcharge
        # factor in [0.90, 1.05] so it is *related* to products.price but not
        # identical (list price can change; orders keep the charged price).
        catalog_price = price_by_id[product_id]
        unit_price = round(catalog_price * rng.uniform(0.90, 1.05), 2)

        # total_amount: always quantity * unit_price, rounded to 2 decimals.
        # Never independently sampled.
        total_amount = round(quantity * unit_price, 2)

        # order_status: Completed ~70% / Pending ~18% / Cancelled ~12%.
        order_status = rng.choices(
            ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1
        )[0]

        # payment_date:
        #   Pending    → null (blank in the CSV)
        #   otherwise  → a DATE on or after order_date, never in the future.
        # Same-day payment is allowed when order_date is today; otherwise the
        # offset is 0–14 days, capped at TODAY.
        if order_status == "Pending":
            payment_date: str | None = None
        else:
            max_lag = min(14, (TODAY - order_date).days)
            lag = rng.randint(0, max_lag) if max_lag > 0 else 0
            payment_date = (order_date + timedelta(days=lag)).isoformat()

        rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_status": order_status,
                "payment_date": payment_date,
            }
        )

    return pd.DataFrame(rows)


# ===========================================================================
# Quality-issue injection (Prompt 3)
# ===========================================================================
# Generate clean, then mutate a disjoint subset. One issue per dirty row so
# Silver checks map 1:1. Products are left clean on purpose.


def _disjoint_index_groups(
    rng: random.Random, n_rows: int, sizes: list[int]
) -> list[list[int]]:
    """Sample disjoint index lists whose lengths equal `sizes`."""
    needed = sum(sizes)
    picked = rng.sample(range(n_rows), needed)
    groups: list[list[int]] = []
    start = 0
    for size in sizes:
        groups.append(picked[start : start + size])
        start += size
    return groups


def inject_customer_issues(
    df: pd.DataFrame, seed: int = SEED + 10
) -> pd.DataFrame:
    """Mutate a disjoint subset of customer rows with seeded defects.

    Prints an injection summary so counts can be verified against the brief.
    """
    rng = random.Random(seed)
    df = df.copy()
    n_rows = len(df)

    null_email_idx, dup_victim_idx = _disjoint_index_groups(
        rng, n_rows, [N_NULL_EMAIL, N_DUP_CUSTOMER_ID]
    )

    # Donors for duplicate PKs must also be disjoint from both dirty groups
    # so a NULL-email row is never also a uniqueness failure.
    dirty = set(null_email_idx + dup_victim_idx)
    donor_pool = [i for i in range(n_rows) if i not in dirty]
    dup_donor_idx = rng.sample(donor_pool, N_DUP_CUSTOMER_ID)

    # ------------------------------------------------------------------
    # INJECTION: 50 rows with NULL email
    # SILVER CHECK: src/silver/01_quality_completeness.py
    # WHY: Completeness should FLAG rows where a required field (email) is
    #      missing. Bronze ingested the null as-is; Silver must not drop
    #      the row — it writes quality_check_result = fail/completeness.
    # ------------------------------------------------------------------
    df.loc[null_email_idx, "email"] = pd.NA

    # ------------------------------------------------------------------
    # INJECTION: 10 rows with duplicate customer_id
    # SILVER CHECK: src/silver/02_quality_uniqueness.py
    # WHY: Uniqueness should FLAG rows whose PK already exists on another
    #      row. We overwrite 10 victim PKs with 10 donor PKs (in-place, so
    #      the file stays at 10,000 rows). The 10 donor rows keep their
    #      original attributes; only the ID collides.
    #      NOTE: a uniqueness check that flags *every* row in a duplicate
    #      PK group will mark 20 rows (10 victims + 10 donors). The 10
    #      counted here are the rows we mutated.
    # ------------------------------------------------------------------
    for victim, donor in zip(dup_victim_idx, dup_donor_idx):
        df.loc[victim, "customer_id"] = df.loc[donor, "customer_id"]

    n_null_email = int(df["email"].isna().sum())
    id_counts = df["customer_id"].value_counts()
    n_dup_ids = int((id_counts > 1).sum())
    n_rows_in_dup_groups = int(id_counts[id_counts > 1].sum())
    n_clean = n_rows - N_NULL_EMAIL - N_DUP_CUSTOMER_ID

    print("=" * 60)
    print("customers.csv — quality-issue injection summary")
    print("=" * 60)
    print(f"  NULL email rows injected:              {n_null_email:>6}  (target {N_NULL_EMAIL})")
    print(f"  duplicate customer_id rows injected:   {N_DUP_CUSTOMER_ID:>6}  (target {N_DUP_CUSTOMER_ID})")
    print(f"    customer_ids that now appear twice:  {n_dup_ids:>6}")
    print(f"    rows Silver uniqueness will see:     {n_rows_in_dup_groups:>6}  (victims + donors)")
    print(f"  remaining clean rows:                  {n_clean:>6}  (target ~9,940)")
    print(f"  total rows:                            {n_rows:>6}")
    print("=" * 60)

    return df


def inject_product_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Products stay clean.

    SILVER CHECK: src/silver/04_quality_referential_integrity.py
    WHY: A clean product dimension is the control table. Order rows with
         product_id not in this file (injected later) are true orphans;
         they are not explained by missing products. Completeness /
         uniqueness on products should report zero failures.
    """
    print("=" * 60)
    print("products.csv — quality-issue injection summary")
    print("=" * 60)
    print("  No issue-rows injected (control file for Silver FK checks).")
    print(f"  remaining clean rows:                  {len(df):>6}")
    print(f"  total rows:                            {len(df):>6}")
    print("=" * 60)
    return df


def inject_order_issues(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    seed: int = SEED + 11,
) -> pd.DataFrame:
    """Mutate a disjoint subset of order rows with seeded defects."""
    rng = random.Random(seed)
    df = orders.copy()
    # Nullable integer dtype so PK/FK nulls write as blank, not 123.0
    df["order_id"] = df["order_id"].astype("Int64")
    df["customer_id"] = df["customer_id"].astype("Int64")
    df["product_id"] = df["product_id"].astype("Int64")
    n_rows = len(df)

    (
        null_cust_idx,
        null_prod_idx,
        orphan_cust_idx,
        orphan_prod_idx,
        dup_victim_idx,
    ) = _disjoint_index_groups(
        rng,
        n_rows,
        [
            N_NULL_ORDER_CUSTOMER_ID,
            N_NULL_ORDER_PRODUCT_ID,
            N_ORPHAN_CUSTOMER_ID,
            N_ORPHAN_PRODUCT_ID,
            N_DUP_ORDER_ID,
        ],
    )

    dirty = set(
        null_cust_idx
        + null_prod_idx
        + orphan_cust_idx
        + orphan_prod_idx
        + dup_victim_idx
    )
    donor_pool = [i for i in range(n_rows) if i not in dirty]
    dup_donor_idx = rng.sample(donor_pool, N_DUP_ORDER_ID)

    existing_customer_ids = set(
        customers["customer_id"].dropna().astype(int).tolist()
    )
    existing_product_ids = set(products["product_id"].astype(int).tolist())
    # IDs guaranteed not to exist in the parent files.
    orphan_customer_start = max(existing_customer_ids) + 1
    orphan_product_start = max(existing_product_ids) + 1

    # ------------------------------------------------------------------
    # INJECTION: 100 rows with NULL customer_id
    # SILVER CHECK: src/silver/01_quality_completeness.py
    # WHY: Completeness should FLAG orders missing a required FK. This is
    #      a null, not an orphan — referential-integrity should not be
    #      the check that catches these (RI runs on non-null FKs).
    # ------------------------------------------------------------------
    df.loc[null_cust_idx, "customer_id"] = pd.NA

    # ------------------------------------------------------------------
    # INJECTION: 200 rows with NULL product_id
    # SILVER CHECK: src/silver/01_quality_completeness.py
    # WHY: Same completeness rule on the product FK. Kept disjoint from
    #      the NULL customer_id rows so the two counts stay independently
    #      verifiable.
    # ------------------------------------------------------------------
    df.loc[null_prod_idx, "product_id"] = pd.NA

    # ------------------------------------------------------------------
    # INJECTION: 50 rows with customer_id not present in customers
    # SILVER CHECK: src/silver/04_quality_referential_integrity.py
    # WHY: RI should FLAG a non-null FK that has no matching parent PK.
    #      Values start at max(customer_id)+1 so they cannot collide with
    #      a real customer (including the duplicated PKs).
    # ------------------------------------------------------------------
    for offset, idx in enumerate(orphan_cust_idx):
        df.loc[idx, "customer_id"] = orphan_customer_start + offset

    # ------------------------------------------------------------------
    # INJECTION: 30 rows with product_id not present in products
    # SILVER CHECK: src/silver/04_quality_referential_integrity.py
    # WHY: Same RI rule on product_id. Values start at max(product_id)+1
    #      (501+). Products themselves were left clean, so these are
    #      genuine orphans, not a missing dimension row.
    # ------------------------------------------------------------------
    for offset, idx in enumerate(orphan_prod_idx):
        df.loc[idx, "product_id"] = orphan_product_start + offset

    # ------------------------------------------------------------------
    # INJECTION: 20 rows with duplicate order_id
    # SILVER CHECK: src/silver/02_quality_uniqueness.py
    # WHY: Uniqueness should FLAG orders whose PK already exists. 20
    #      victims copy 20 donor order_ids (file stays at 100,000 rows).
    #      Silver will see 40 rows in duplicate PK groups (20+20).
    # ------------------------------------------------------------------
    for victim, donor in zip(dup_victim_idx, dup_donor_idx):
        df.loc[victim, "order_id"] = df.loc[donor, "order_id"]

    n_null_cust = int(df["customer_id"].isna().sum())
    n_null_prod = int(df["product_id"].isna().sum())
    non_null_cust = df["customer_id"].dropna()
    n_orphan_cust = int((~non_null_cust.isin(existing_customer_ids)).sum())
    non_null_prod = df["product_id"].dropna()
    n_orphan_prod = int((~non_null_prod.isin(existing_product_ids)).sum())
    id_counts = df["order_id"].value_counts()
    n_dup_ids = int((id_counts > 1).sum())
    n_rows_in_dup_groups = int(id_counts[id_counts > 1].sum())
    n_injected = (
        N_NULL_ORDER_CUSTOMER_ID
        + N_NULL_ORDER_PRODUCT_ID
        + N_ORPHAN_CUSTOMER_ID
        + N_ORPHAN_PRODUCT_ID
        + N_DUP_ORDER_ID
    )
    n_clean = n_rows - n_injected

    print("=" * 60)
    print("orders.csv — quality-issue injection summary")
    print("=" * 60)
    print(f"  NULL customer_id rows injected:        {n_null_cust:>6}  (target {N_NULL_ORDER_CUSTOMER_ID})")
    print(f"  NULL product_id rows injected:         {n_null_prod:>6}  (target {N_NULL_ORDER_PRODUCT_ID})")
    print(f"  orphan customer_id rows injected:      {n_orphan_cust:>6}  (target {N_ORPHAN_CUSTOMER_ID})")
    print(f"  orphan product_id rows injected:       {n_orphan_prod:>6}  (target {N_ORPHAN_PRODUCT_ID})")
    print(f"  duplicate order_id rows injected:      {N_DUP_ORDER_ID:>6}  (target {N_DUP_ORDER_ID})")
    print(f"    order_ids that now appear twice:     {n_dup_ids:>6}")
    print(f"    rows Silver uniqueness will see:     {n_rows_in_dup_groups:>6}  (victims + donors)")
    print(f"  remaining clean rows:                  {n_clean:>6}  (target ~99,300)")
    print(f"  total rows:                            {n_rows:>6}")
    print("=" * 60)

    return df


def _print_customer_checks(df: pd.DataFrame) -> None:
    print(f"Wrote {len(df):,} rows → {CUSTOMERS_PATH}")
    print("Segment mix (expect ~20 / 50 / 30):")
    print(df["customer_segment"].value_counts(normalize=True).mul(100).round(1).to_string())
    print("Mean lifetime_value by segment (Premium should be highest):")
    print(df.groupby("customer_segment")["lifetime_value"].mean().round(2).to_string())
    print("signup_date range:", df["signup_date"].min(), "→", df["signup_date"].max())
    print("Unique emails:", df["email"].nunique(), "/", len(df))


def _print_product_checks(df: pd.DataFrame) -> None:
    print(f"Wrote {len(df):,} rows → {PRODUCTS_PATH}")
    print("Category counts:")
    print(df["category"].value_counts().to_string())
    print("price range:", df["price"].min(), "→", df["price"].max())
    print("Rows with cost >= price (expect 0):", int((df["cost"] >= df["price"]).sum()))
    print("Unique product_name:", df["product_name"].nunique(), "/", len(df))


def _print_order_checks(
    orders: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame
) -> None:
    print(f"Wrote {len(orders):,} rows → {ORDERS_PATH}")
    print("Status mix (expect ~70 / 18 / 12):")
    print(orders["order_status"].value_counts(normalize=True).mul(100).round(1).to_string())

    print("Post-injection FK / PK counts are in the injection summary above.")

    recomputed = (orders["quantity"] * orders["unit_price"]).round(2)
    mismatches = int((recomputed != orders["total_amount"]).sum())
    print("total_amount != quantity * unit_price (expect 0):", mismatches)

    pending = orders["order_status"] == "Pending"
    pending_with_pay = int(pending.sum() - orders.loc[pending, "payment_date"].isna().sum())
    other_missing_pay = int(orders.loc[~pending, "payment_date"].isna().sum())
    print("Pending rows with payment_date (expect 0):", pending_with_pay)
    print("Non-Pending rows missing payment_date (expect 0):", other_missing_pay)
    print("order_date range:", orders["order_date"].min(), "→", orders["order_date"].max())
    print("Distinct customers in orders:", orders["customer_id"].nunique(), "/", len(customers))
    print("Distinct products in orders:", orders["product_id"].nunique(), "/", len(products))


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Generating {N_CUSTOMERS:,} customers (seed={SEED})...")
    customers = generate_customers()
    customers = inject_customer_issues(customers)
    customers.to_csv(CUSTOMERS_PATH, index=False, na_rep="")
    _print_customer_checks(customers)
    print()

    print(f"Generating {N_PRODUCTS:,} products (seed={SEED + 1})...")
    products = generate_products()
    products = inject_product_issues(products)
    products.to_csv(PRODUCTS_PATH, index=False)
    _print_product_checks(products)
    print()

    print(f"Generating {N_ORDERS:,} orders (seed={SEED + 2})...")
    orders = generate_orders(customers, products)
    orders = inject_order_issues(orders, customers, products)
    # na_rep="" so null FKs / Pending payment_date are blank, not the text "nan".
    orders.to_csv(ORDERS_PATH, index=False, na_rep="")
    _print_order_checks(orders, customers, products)


if __name__ == "__main__":
    main()
