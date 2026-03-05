-- ============================================================
-- dim_service_plan.sql
-- Service plan dimension
-- ============================================================

with source as (
    select * from {{ ref('stg_service_plans') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['plan_id']) }}  as plan_key,

        plan_id,
        plan_name,
        plan_type,
        monthly_fee,
        data_limit_gb,
        call_minutes_limit,
        sms_limit,
        plan_tier

    from source
)

select * from final