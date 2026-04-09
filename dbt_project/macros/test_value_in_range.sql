-- =============================================================================
-- test_value_in_range.sql — Custom Generic Test (AI-7)
-- =============================================================================
-- Fails if any value in the column falls outside [min_val, max_val].
-- Useful for detecting data corruption or injection of invalid
-- numeric values (e.g., negative quantities, prices exceeding cap).
--
-- Usage in schema.yml:
--   columns:
--     - name: quantity
--       tests:
--         - value_in_range:
--             min_val: 1
--             max_val: 10
--             config:
--               severity: warn
-- =============================================================================

{% test value_in_range(model, column_name, min_val, max_val) %}

    select
        {{ column_name }} as out_of_range_value
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < {{ min_val }} or {{ column_name }} > {{ max_val }})

{% endtest %}
