-- ============================================================
-- stg_service_usage.sql
-- ============================================================

with source as (
    select * from {{ source('staging', 'stg_service_usage') }}
),

cleaned as (
    select
        usage_id,
        customer_id,
        subscription_id,
        usage_date,

        -- Ensure no negatives (defensive)
        greatest(data_used_gb, 0)                   as data_used_gb,
        greatest(call_minutes_used, 0)              as call_minutes_used,
        greatest(sms_used, 0)                       as sms_used,
        greatest(overage_charges, 0)                as overage_charges,

        _loaded_at

    from source
    where usage_id is not null
        and customer_id is not null
        and usage_date is not null
)

select * from cleaned