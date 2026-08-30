# Data generation notes — customers.csv

## Purpose

Produce **synthetic** Bronze-layer source files. No real customer PII.

This pass generates **customers.csv only** (10,000 rows). Orders and products
will be generated in a later step so foreign keys can be drawn from this file.

## How to run

```bash
pip install faker pandas
python src/data_generation/generate_sample_data.py
```

Output: `data/customers.csv`

Re-running with the same `SEED` (42) produces the same file.

## Field logic

| Field | Type | Generation |
|---|---|---|
| `customer_id` | INT PK | Sequential 1 .. 10000, no gaps |
| `customer_name` | STRING | Faker name; locale follows `country` (Japan uses `en_US` so names stay romanized) |
| `email` | STRING | `{ascii_slug(name)}{customer_id}@{domain}` — unique, name-aligned |
| `country` | STRING | Weighted mix (US 28%, UK 12%, DE 10%, …) — not world-population weights |
| `signup_date` | DATE | Uniform between `2020-01-01` and `date.today()` |
| `customer_segment` | STRING | Premium 20% / Standard 50% / Basic 30% |
| `lifetime_value` | DECIMAL(10,2) | Segment ranges with **overlap** so correlation is loose |

## LTV ranges (USD, overlapping on purpose)

- Premium: 800 – 4500
- Standard: 150 – 1800
- Basic: 25 – 700

## Assumptions to verify when you test

1. `signup_date` max equals the date you ran the script (upper bound is `today`).
2. Segment shares are *approximately* 20/50/30 — sampling noise of ~1–2 pp is expected.
3. Mean LTV: Premium > Standard > Basic, but individual Basic rows can exceed some Standard rows.
4. Emails contain the numeric `customer_id` suffix; that is intentional uniqueness, not a source-system convention.
5. Country list is 13 markets, not a full ISO list.
