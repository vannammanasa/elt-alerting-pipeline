-- =============================================================================
-- int_daily_sales.sql — Intermediate Model (AI-4)
-- =============================================================================
-- Aggregates transactions to daily grain.  This model is a key target
-- for the volume anomaly failure injection (AI-8): a >50% drop in
-- daily row count is classified as a critical failure by the severity
-- rules defined in Chapter 3, Table 3.1.
--
-- Materialised as: VIEW
-- Source:           intermediate.int_transactions_enriched
-- =============================================================================

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

daily as (

    select
        -- ── Date grain ──────────────────────────────────────────────
        date_trunc('day', event_timestamp)::date    as sale_date,

        -- ── Volume metrics ──────────────────────────────────────────
        count(*)                                     as transaction_count,
        count(distinct customer_id)                  as unique_customers,
        sum(quantity)                                 as total_units_sold,

        -- ── Revenue metrics ─────────────────────────────────────────
        round(sum(total_amount)::numeric, 2)         as daily_revenue,
        round(avg(total_amount)::numeric, 2)         as avg_order_value,
        round(min(total_amount)::numeric, 2)         as min_order_value,
        round(max(total_amount)::numeric, 2)         as max_order_value,

        -- ── Composition metrics ─────────────────────────────────────
        count(*) filter (where is_completed)         as completed_orders,
        count(*) filter (where order_status = 'cancelled')  as cancelled_orders,
        count(*) filter (where is_high_value)        as high_value_orders,
        count(*) filter (where is_returning_customer) as returning_customer_orders,

        -- ── Category breakdown (top category by revenue) ────────────
        mode() within group (order by product_category) as top_category,

        -- ── Null rate tracking (for quality monitoring) ─────────────
        round(
            count(*) filter (where shipping_country is null)::numeric
            / nullif(count(*), 0), 4
        ) as shipping_country_null_rate

    from enriched
    group by date_trunc('day', event_timestamp)::date

)

select * from daily
