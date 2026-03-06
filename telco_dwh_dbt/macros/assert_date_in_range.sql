-- ============================================================
-- Generic test: assert_date_in_range
-- Usage in schema.yml:
--   - name: billing_date
--     tests:
--       - assert_date_in_range:
--           min_date: '2024-01-01'
--           max_date: '2024-12-31'
-- ============================================================

{% test assert_date_in_range(model, column_name, min_date, max_date) %}

select *
from {{ model }}
where {{ column_name }} < '{{ min_date }}'::date
   or {{ column_name }} > '{{ max_date }}'::date

{% endtest %}