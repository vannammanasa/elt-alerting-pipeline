-- =============================================================================
-- mart_customer_summary.sql — Mart Model (AI-5)
-- =============================================================================
-- Customer-level aggregation providing lifetime metrics per customer.
-- Useful for validating customer_id integrity and testing the
-- accepted_values / uniqueness quality checks in Phase 3.
--
-- Materialised as: TABLE
-- Source:           intermediate.int_transactions_enriched
-- Target schema:    transformed
-- =============================================================================

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

customer_agg as (

    select
        customer_id,

        -- ── Activity metrics ────────────────────────────────────────
        count(*)                                             as total_orders,
        count(distinct date_trunc('day', event_timestamp))   as active_days,
        min(event_timestamp)                                 as first_order_at,
        max(event_timestamp)                                 as last_order_at,

        -- ── Revenue metrics ─────────────────────────────────────────
        round(sum(total_amount)::numeric, 2)                 as lifetime_revenue,
        round(avg(total_amount)::numeric, 2)                 as avg_order_value,
        round(max(total_amount)::numeric, 2)                 as max_order_value,
        sum(quantity)                                         as total_units,

        -- ── Behavioural flags ───────────────────────────────────────
        bool_or(is_returning_customer)                       as is_returning,
        count(*) filter (where is_completed)                 as completed_orders,
        count(*) filter (where order_status = 'cancelled')   as cancelled_orders,
        count(*) filter (where is_high_value)                as high_value_orders,

        -- ── Preferred attributes ────────────────────────────────────
        mode() within group (order by product_category)      as preferred_category,
        mode() within group (order by payment_method)        as preferred_payment,
        mode() within group (order by shipping_country)      as primary_country

    from enriched
    group by customer_id

)

select * from customer_agg
