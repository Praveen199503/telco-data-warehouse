-- ============================================================
-- stg_service_plans.sql
-- ============================================================

with source as (
    select * from {{ source('staging', 'stg_service_plans') }}
),

cleaned as (
    select
        plan_id,
        trim(plan_name)                             as plan_name,
        lower(plan_type)                            as plan_type,
        monthly_fee,
        data_limit_gb,

        -- -1 means unlimited in source, convert to NULL for clarity
        nullif(call_minutes_limit, -1)              as call_minutes_limit,
        nullif(sms_limit, -1)                       as sms_limit,

        -- Derived: plan tier based on fee
        case
            when monthly_fee < 15  then 'Basic'
            when monthly_fee < 35  then 'Standard'
            else                        'Premium'
        end                                         as plan_tier,

        _loaded_at

    from source
)

select * from cleaned