-- =============================================================================
-- stg_transactions.sql — Staging Model (AI-3)
-- =============================================================================
-- Cleans, type-casts, and deduplicates raw transaction records.
-- This is the first transformation boundary in the ELT pipeline and
-- the primary target for quality validation tests in Phase 3.
--
-- Materialised as: VIEW (always reflects latest raw data)
-- Source:           raw.raw_transactions
-- Target schema:    staging
-- =============================================================================

with source as (

    select * from {{ source('raw', 'raw_transactions') }}

),

-- Remove exact duplicates (same transaction_id), keeping the most
-- recently ingested record.  This handles the duplicate injection
-- failure scenario (AI-8) at the staging boundary.
deduplicated as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by _ingestion_timestamp desc
        ) as _row_num
    from source

),

cleaned as (

    select
        -- ── Primary key ─────────────────────────────────────────────
        transaction_id,

        -- ── Timestamps ──────────────────────────────────────────────
        event_timestamp::timestamp with time zone   as event_timestamp,

        -- ── Customer attributes ─────────────────────────────────────
        customer_id::integer                         as customer_id,
        lower(trim(customer_email))                  as customer_email,
        is_returning_customer::boolean               as is_returning_customer,

        -- ── Product attributes ──────────────────────────────────────
        product_id::integer                          as product_id,
        lower(trim(product_category))                as product_category,

        -- ── Order details ───────────────────────────────────────────
        quantity::integer                            as quantity,
        round(unit_price::numeric, 2)                as unit_price,
        round(total_amount::numeric, 2)              as total_amount,
        lower(trim(payment_method))                  as payment_method,
        lower(trim(order_status))                    as order_status,

        -- ── Shipping ────────────────────────────────────────────────
        upper(trim(shipping_country))                as shipping_country,

        -- ── Metadata (carried forward for lineage) ──────────────────
        _ingestion_timestamp,
        _source_id,
        _batch_id

    from deduplicated
    where _row_num = 1

)

select * from cleaned
