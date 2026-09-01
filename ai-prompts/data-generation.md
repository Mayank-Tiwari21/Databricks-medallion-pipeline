# Prompt — data generation

## Prompt 1 (customers)

Write a Python script (using faker + pandas) to generate customers.csv with 10,000 rows:
customer_id (INT, sequential PK), customer_name, email, country, signup_date (DATE, between
2020-01-01 and today), customer_segment (Premium/Standard/Basic, weighted ~20/50/30), lifetime_value
(DECIMAL, correlated loosely with segment — Premium higher). Use realistic names/emails/countries.
Add comments explaining each field's generation logic.

## Prompt 2 (orders + products)

Now do the same for orders.csv (100,000 rows) and products.csv (500 rows), using the schema I gave
you in the context above. orders.customer_id and orders.product_id should reference the
customers/products IDs you just generated. total_amount = quantity * unit_price. payment_date should
be null when order_status = 'Pending', and populated (after order_date) otherwise.

## Prompt 3 (seeded quality issues)

Modify the three generator scripts to intentionally introduce these data quality issues, and add
comments at each injection point explaining WHY it's there (so a reviewer can map it back to the
Silver layer check that should catch it):

customers.csv: 50 rows with NULL email; 10 rows with duplicate customer_id.
orders.csv: 100 rows with NULL customer_id; 200 rows with NULL product_id; 50 rows with customer_id
not present in customers; 30 rows with product_id not present in products; 20 duplicate order_id rows.

Keep the remaining ~99,300 order rows and ~9,940 customer rows clean. Print a summary at the end of
each script showing how many issue-rows of each type were injected, so I can verify counts.

## Prompt 4 (DATA_GENERATION_NOTES.md)

Write DATA_GENERATION_NOTES.md summarizing: what each script generates, the exact quality issues
injected and their row counts, why these specific issues were chosen (map each to completeness/
uniqueness/referential-integrity), and how to regenerate the data with a fixed random seed for
reproducibility.
