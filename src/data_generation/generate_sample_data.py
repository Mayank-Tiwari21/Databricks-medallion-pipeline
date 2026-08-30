"""
Generate synthetic e-commerce customer data for the Bronze layer.

Output
------
data/customers.csv  — 10,000 rows, one customer per row.

All values are synthetic (Faker + random). No real customer PII is used.

Usage
-----
    python src/data_generation/generate_sample_data.py

Assumptions (flag these when you test)
--------------------------------------
- customer_id starts at 1 and is gap-free (1 .. 10000).
- signup_date upper bound is date.today() on the machine that runs this script.
- lifetime_value is USD, 2 decimal places; ranges overlap across segments on
  purpose so the correlation is loose, not a deterministic lookup.
- Emails are unique but are *constructed* from the name (not Faker.email()),
  so they look like they belong to that person.
- Country mix is a typical English-speaking e-commerce distribution, not
  world-population weights.
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
# Fixed seed so re-running the script produces the same 10,000 customers.
# Change SEED if you want a different but still reproducible sample.
SEED = 42
N_CUSTOMERS = 10_000

# signup_date window: 2020-01-01 through today (inclusive).
SIGNUP_START = date(2020, 1, 1)
SIGNUP_END = date.today()

# Project root = databricks-medallion-pipeline/  (two levels above this file)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data" / "customers.csv"

# ---------------------------------------------------------------------------
# Country mix
# ---------------------------------------------------------------------------
# Weighted toward markets that commonly appear in English-language e-commerce
# datasets. Weights sum to 1.0.
# Locale is used only to generate a culturally matching name; emails are
# ASCII-folded so the CSV stays easy to load in Databricks.
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

# ---------------------------------------------------------------------------
# customer_segment: Premium / Standard / Basic, weighted ~20 / 50 / 30
# ---------------------------------------------------------------------------
SEGMENTS = ["Premium", "Standard", "Basic"]
SEGMENT_WEIGHTS = [0.20, 0.50, 0.30]

# ---------------------------------------------------------------------------
# lifetime_value ranges (USD) — loosely correlated with segment
# ---------------------------------------------------------------------------
# Ranges overlap so a high-value Basic customer can out-earn a low-value
# Standard customer. Premium is still centered higher.
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


def _ascii_slug(text: str) -> str:
    """Fold accented characters to ASCII and keep letters/digits only.

    Used to turn 'José García' into 'jose.garcia' for the email local-part.
    """
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = "".join(ch.lower() if ch.isalnum() else "." for ch in ascii_text)
    # Collapse repeated dots and trim.
    parts = [p for p in cleaned.split(".") if p]
    return ".".join(parts)


def _email_from_name(name: str, customer_id: int, rng: random.Random) -> str:
    """Build a unique, name-based email.

    Pattern: {first}.{last}{id}@{domain}  e.g. jane.doe142@gmail.com
    The numeric suffix (customer_id) guarantees uniqueness across 10k rows
    without needing Faker's unique provider.
    """
    slug = _ascii_slug(name)
    if not slug:
        slug = f"customer.{customer_id}"
    domain = rng.choice(EMAIL_DOMAINS)
    return f"{slug}{customer_id}@{domain}"


def _random_signup_date(rng: random.Random) -> date:
    """Uniform random date between SIGNUP_START and SIGNUP_END (inclusive)."""
    span_days = (SIGNUP_END - SIGNUP_START).days
    offset = rng.randint(0, span_days)
    return SIGNUP_START + timedelta(days=offset)


def _lifetime_value(segment: str, rng: random.Random) -> float:
    """Draw LTV from the segment's range, rounded to 2 decimal places."""
    low, high = LTV_RANGES[segment]
    value = rng.uniform(low, high)
    return round(value, 2)


def generate_customers(n: int = N_CUSTOMERS, seed: int = SEED) -> pd.DataFrame:
    """Return a DataFrame of `n` synthetic customers."""
    rng = random.Random(seed)

    # One Faker instance per locale, all seeded so names are reproducible.
    fakers: dict[str, Faker] = {}
    for locale in set(COUNTRY_LOCALE.values()):
        fake = Faker(locale)
        fake.seed_instance(seed)
        fakers[locale] = fake

    rows: list[dict] = []

    for customer_id in range(1, n + 1):
        # customer_id: sequential integer primary key, starting at 1.
        # No gaps, no prefixes — warehouse-style surrogate key.

        # country: weighted choice from COUNTRY_SPEC (not uniform).
        country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]

        # customer_name: Faker name from the locale that matches `country`.
        # Japan uses en_US so names stay romanized (see COUNTRY_SPEC comment).
        locale = COUNTRY_LOCALE[country]
        customer_name = fakers[locale].name()

        # email: derived from the name + customer_id so it is unique and
        # visually tied to the person. Domain is a common consumer provider.
        email = _email_from_name(customer_name, customer_id, rng)

        # signup_date: uniform DATE between 2020-01-01 and today.
        signup_date = _random_signup_date(rng)

        # customer_segment: Premium ~20%, Standard ~50%, Basic ~30%.
        customer_segment = rng.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]

        # lifetime_value: DECIMAL(10,2) drawn from a segment-specific range.
        # Correlation is loose because the three ranges overlap.
        lifetime_value = _lifetime_value(customer_segment, rng)

        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "country": country,
                "signup_date": signup_date.isoformat(),  # YYYY-MM-DD
                "customer_segment": customer_segment,
                "lifetime_value": lifetime_value,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    print(f"Generating {N_CUSTOMERS:,} synthetic customers (seed={SEED})...")
    df = generate_customers()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    # index=False so customer_id is a data column, not a pandas index.
    df.to_csv(OUTPUT_PATH, index=False)

    # Quick sanity checks printed so you can spot-check without opening Excel.
    print(f"Wrote {len(df):,} rows → {OUTPUT_PATH}")
    print()
    print("Segment mix (expect ~20 / 50 / 30):")
    print(df["customer_segment"].value_counts(normalize=True).mul(100).round(1).to_string())
    print()
    print("Mean lifetime_value by segment (Premium should be highest):")
    print(df.groupby("customer_segment")["lifetime_value"].mean().round(2).to_string())
    print()
    print("signup_date range:", df["signup_date"].min(), "→", df["signup_date"].max())
    print("Unique emails:", df["email"].nunique(), "/", len(df))
    print("Unique customer_id:", df["customer_id"].nunique(), "/", len(df))


if __name__ == "__main__":
    main()
