# Data model

## Customers (source)

| Column | Type | Notes |
|---|---|---|
| customer_id | INT | PK, sequential |
| customer_name | STRING | Synthetic |
| email | STRING | Unique, synthetic |
| country | STRING | Weighted market mix |
| signup_date | DATE | 2020-01-01 .. today |
| customer_segment | STRING | Premium / Standard / Basic |
| lifetime_value | DECIMAL | Loosely correlated with segment |

Orders and products schemas will be documented when those generators are added.
