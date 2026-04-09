-- =============================================================================
-- test_freshness_hours.sql — Custom Generic Test (AI-7)
-- =============================================================================
-- Fails if the most recent record in the timestamp column is older
-- than max_hours from the current time.  Detects the freshness delay
-- failure injection scenario (AI-8) where data arrival is artificially
-- delayed beyond the SLA window.
--
-- Usage in schema.yml:
--   models:
--     - name: stg_transactions
--       tests:
--         - freshness_hours:
--             timestamp_column: _ingestion_timestamp
--             max_hours: 24
--             config:
--               severity: warn
-- =============================================================================

{% test freshness_hours(model, timestamp_column, max_hours) %}

    select
        max({{ timestamp_column }}) as latest_record,
        current_timestamp as checked_at,
        extract(epoch from (
            current_timestamp - max({{ timestamp_column }})
        )) / 3600.0 as hours_since_latest,
        {{ max_hours }} as max_allowed_hours
    from {{ model }}
    having extract(epoch from (
        current_timestamp - max({{ timestamp_column }})
    )) / 3600.0 > {{ max_hours }}

{% endtest %}
