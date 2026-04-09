-- =============================================================================
-- test_row_count_threshold.sql — Custom Generic Test (AI-7)
-- =============================================================================
-- Fails if the model has fewer rows than the specified minimum.  This
-- test detects the volume anomaly failure injection scenario (AI-8):
-- a >50% drop in expected row count triggers a CRITICAL alert.
--
-- Usage in schema.yml:
--   models:
--     - name: mart_transactions
--       tests:
--         - row_count_threshold:
--             min_count: 5000
--             config:
--               severity: error
-- =============================================================================

{% test row_count_threshold(model, min_count) %}

    select
        count(*) as row_count,
        {{ min_count }} as min_expected
    from {{ model }}
    having count(*) < {{ min_count }}

{% endtest %}
