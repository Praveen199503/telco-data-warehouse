-- ============================================================
-- fact_service_usage.sql
-- Measures: data, calls, sms, overage per customer per week
-- ============================================================

with usage as (
    select * from {{ ref('stg_service_usage') }}
),

customers as (
    -- Only join to CURRENT customer records (is_current = true)
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
        -- Surrogate key for this fact row
        {{ dbt_utils.generate_surrogate_key(['u.usage_id']) }}
                                                     as usage_key,

        -- Foreign keys to dimensions
        c.customer_key,
        p.plan_key,
        d.date_key,

        -- Measures
        u.data_used_gb,
        u.call_minutes_used,
        u.sms_used,
        u.overage_charges,

        -- Derived measure: utilisation %
        -- How much of their data limit did they use?
        case
            when p.data_limit_gb > 0
            then round(
                (u.data_used_gb / (p.data_limit_gb / 4.0)) * 100
            , 2)
            else 0
        end                                          as data_utilization_pct

    from usage u
    inner join subscriptions s
        on u.subscription_id = s.subscription_id
    inner join customers c
        on u.customer_id = c.customer_id
    inner join plans p
        on s.plan_id = p.plan_id
    inner join dates d
        on u.usage_date = d.full_date
)

select * from final