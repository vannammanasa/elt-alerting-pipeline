-- =============================================================================
-- test_null_rate_below.sql — Custom Generic Test (AI-7)
-- =============================================================================
-- Fails if the null rate for a given column exceeds the threshold.
-- Designed for non-essential nullable columns where a sudden spike
-- in nulls indicates a data quality issue (WARNING severity).
--
-- Usage in schema.yml:
--   columns:
--     - name: shipping_country
--       tests:
--         - null_rate_below:
--             max_null_rate: 0.10
--             config:
--               severity: warn
-- =============================================================================

{% test null_rate_below(model, column_name, max_null_rate) %}

    select
        round(
            count(*) filter (where {{ column_name }} is null)::numeric
            / nullif(count(*), 0),
            4
        ) as actual_null_rate,
        {{ max_null_rate }} as max_allowed
    from {{ model }}
    having round(
        count(*) filter (where {{ column_name }} is null)::numeric
        / nullif(count(*), 0),
        4
    ) > {{ max_null_rate }}

{% endtest %}
