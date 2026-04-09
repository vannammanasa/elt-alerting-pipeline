-- =============================================================================
-- mart_transactions.sql — Mart Model (AI-5)
-- =============================================================================
-- Final analytical fact table containing all enriched transaction records.
-- Materialised as a TABLE in the 'transformed' schema for downstream
-- analytical queries.  Metadata columns are excluded — only business
-- columns are exposed to consumers.
--
-- Materialised as: TABLE
-- Source:           intermediate.int_transactions_enriched
-- Target schema:    transformed
-- =============================================================================

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

final as (

    select
        -- ── Primary key ─────────────────────────────────────────────
        transaction_id,

        -- ── Timestamps ──────────────────────────────────────────────
        event_timestamp,
        date_trunc('day', event_timestamp)::date     as transaction_date,

        -- ── Customer ────────────────────────────────────────────────
        customer_id,
        customer_email,
        is_returning_customer,

        -- ── Product ─────────────────────────────────────────────────
        product_id,
        product_category,

        -- ── Order ───────────────────────────────────────────────────
        quantity,
        unit_price,
        total_amount,
        revenue_band,
        is_high_value,
        payment_method,
        order_status,
        is_completed,

        -- ── Shipping ────────────────────────────────────────────────
        shipping_country,
        is_domestic

    from enriched

)

select * from final
