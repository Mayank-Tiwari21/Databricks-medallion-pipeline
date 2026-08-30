# Prompt — data generation (customers)

Write a Python script (using faker + pandas) to generate customers.csv with 10,000 rows:
customer_id (INT, sequential PK), customer_name, email, country, signup_date (DATE, between
2020-01-01 and today), customer_segment (Premium/Standard/Basic, weighted ~20/50/30), lifetime_value
(DECIMAL, correlated loosely with segment — Premium higher). Use realistic names/emails/countries.
Add comments explaining each field's generation logic.
