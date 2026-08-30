# Data quality strategy

Silver-layer rule: **flag** bad rows in `quality_check_result`. Never silently drop rows.

Checks to implement:

1. Completeness
2. Uniqueness
3. Type validation
4. Referential integrity
5. Business logic
