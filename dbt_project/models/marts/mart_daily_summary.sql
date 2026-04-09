-- =============================================================================
-- mart_daily_summary.sql — Mart Model (AI-5)
-- =============================================================================
-- Materialised daily summary table for trend analysis and volume
-- monitoring.  This table is a primary target for the row count
-- threshold quality tests (Phase 3) — a sudden drop in
-- transaction_count triggers a critical alert under the severity
-- classification rules.
--
-- Materialised as: TABLE
-- Source:           intermediate.int_daily_sales
-- Target schema:    transformed
-- =============================================================================

with daily as (

    select * from {{ ref('int_daily_sales') }}

),

final as (

    select
        sale_date,
        transaction_count,
        unique_customers,
        total_units_sold,
        daily_revenue,
        avg_order_value,
        min_order_value,
        max_order_value,
        completed_orders,
        cancelled_orders,
        high_value_orders,
        returning_customer_orders,
        top_category,
        shipping_country_null_rate,

        -- ── Running metrics (for trend detection) ───────────────────
        round(
            avg(daily_revenue) over (
                order by sale_date
                rows between 6 preceding and current row
            )::numeric, 2
        ) as revenue_7d_moving_avg,

        round(
            avg(transaction_count) over (
                order by sale_date
                rows between 6 preceding and current row
            )::numeric, 1
        ) as volume_7d_moving_avg

    from daily

)

select * from final
