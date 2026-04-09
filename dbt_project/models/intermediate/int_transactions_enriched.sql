-- =============================================================================
-- int_transactions_enriched.sql — Intermediate Model (AI-4)
-- =============================================================================
-- Enriches staged transactions with derived business fields:
--   • revenue_band    (low / medium / high / premium)
--   • order_age_days  (days since event)
--   • is_high_value   (total_amount > £100)
--   • is_domestic     (shipping_country = 'GB')
--
-- These computed columns support the mart-layer aggregations and
-- provide additional columns for quality validation testing.
--
-- Materialised as: VIEW
-- Source:           staging.stg_transactions
-- =============================================================================

with staged as (

    select * from {{ ref('stg_transactions') }}

),

enriched as (

    select
        -- ── Pass-through columns ────────────────────────────────────
        transaction_id,
        event_timestamp,
        customer_id,
        customer_email,
        is_returning_customer,
        product_id,
        product_category,
        quantity,
        unit_price,
        total_amount,
        payment_method,
        order_status,
        shipping_country,
        _ingestion_timestamp,
        _source_id,
        _batch_id,

        -- ── Derived: revenue band ───────────────────────────────────
        case
            when total_amount < 20    then 'low'
            when total_amount < 100   then 'medium'
            when total_amount < 500   then 'high'
            else                           'premium'
        end as revenue_band,

        -- ── Derived: order age in days ──────────────────────────────
        extract(epoch from (
            current_timestamp - event_timestamp
        )) / 86400.0 as order_age_days,

        -- ── Derived: high-value flag ────────────────────────────────
        (total_amount > 100.0) as is_high_value,

        -- ── Derived: domestic vs international ──────────────────────
        (shipping_country = 'GB') as is_domestic,

        -- ── Derived: completed order flag ───────────────────────────
        (order_status = 'completed') as is_completed

    from staged

)

select * from enriched
