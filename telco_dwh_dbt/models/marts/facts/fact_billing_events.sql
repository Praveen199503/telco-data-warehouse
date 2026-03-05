-- ============================================================
-- fact_billing_events.sql
-- Measures: amounts, payment behaviour per customer per month
-- ============================================================

with billing as (
    select * from {{ ref('stg_billing_events') }}
),

customers as (
    select * from {{ ref('dim_customer') }}
    where is_current = true
),

plans as (
    select * from {{ ref('dim_service_plan') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.billing_id']) }}
                                                     as billing_key,

        c.customer_key,
        p.plan_key,
        d.date_key,

        -- Measures
        b.amount_due,
        b.amount_paid,
        b.outstanding_balance,
        b.days_to_payment,
        b.is_overdue,
        b.payment_status,
        b.payment_method

    from billing b
    inner join subscriptions s
        on b.subscription_id = s.subscription_id
    inner join customers c
        on b.customer_id = c.customer_id
    inner join plans p
        on s.plan_id = p.plan_id
    inner join dates d
        on b.billing_date = d.full_date
)

select * from final