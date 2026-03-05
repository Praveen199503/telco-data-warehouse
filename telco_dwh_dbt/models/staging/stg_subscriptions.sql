-- ============================================================
-- stg_subscriptions.sql
-- ============================================================

with source as (
    select * from {{ source('staging', 'stg_subscriptions') }}
),

cleaned as (
    select
        subscription_id,
        customer_id,
        plan_id,
        start_date,

        -- If end_date is null, subscription is still active
        end_date,
        coalesce(end_date, '9999-12-31'::date)      as effective_end_date,

        lower(subscription_status)                  as subscription_status,

        -- Derived: is this subscription currently active?
        case
            when end_date is null then true
            when end_date >= current_date then true
            else false
        end                                         as is_active,

        _loaded_at

    from source
    where subscription_id is not null
)

select * from cleaned