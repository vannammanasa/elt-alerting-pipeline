-- =============================================================================
-- test_schema_has_columns.sql — Custom Generic Test (AI-7)
-- =============================================================================
-- Verifies that a model contains all expected columns.  Detects the
-- schema drift failure injection scenario (AI-8) where columns are
-- added, removed, or renamed.  This is a CRITICAL severity test —
-- missing columns indicate a breaking schema change.
--
-- Usage in schema.yml:
--   models:
--     - name: stg_transactions
--       tests:
--         - schema_has_columns:
--             expected_columns:
--               - transaction_id
--               - event_timestamp
--               - customer_id
--             config:
--               severity: error
-- =============================================================================

{% test schema_has_columns(model, expected_columns) %}

    {%- set query %}
        select column_name
        from information_schema.columns
        where table_schema || '.' || table_name = '{{ model }}'
           or table_name = '{{ model.name }}'
    {% endset -%}

    {# Generate one row per missing column — any rows returned = failure #}
    {% for col in expected_columns %}
    select '{{ col }}' as missing_column
    where not exists (
        select 1
        from information_schema.columns
        where table_name = '{{ model.name }}'
          and column_name = '{{ col }}'
    )
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

{% endtest %}
