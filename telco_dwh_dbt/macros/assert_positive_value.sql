-- ============================================================
-- Generic test: assert_positive_value
-- Usage in schema.yml:
--   - name: amount_due
--     tests:
--       - assert_positive_value
-- Fails if ANY row has a negative value
-- ============================================================

{% test assert_positive_value(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}