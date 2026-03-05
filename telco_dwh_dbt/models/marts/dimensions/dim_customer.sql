-- ============================================================
-- dim_customer.sql
-- Final customer dimension
-- Reads from snapshot which handles SCD 1, 2, 3
--
-- SCD Type 1: email, city → snapshot just overwrites
-- SCD Type 2: current_plan_name → snapshot creates new rows
-- SCD Type 3: previous_plan_name → stored as a column
-- ============================================================

with snapshot as (
    select * from {{ ref('dim_customer_snapshot') }}
),

final as (
    select
        -- Surrogate key (unique per customer per plan period)
        {{ dbt_utils.generate_surrogate_key(
            ['customer_id', 'dbt_valid_from']
        ) }}                                         as customer_key,

        customer_id,
        first_name,
        last_name,
        email,                -- SCD1: always current value
        gender,
        city,                 -- SCD1: always current value
        country,
        age_group,
        registration_date,
        customer_status,
        customer_segment,

        -- SCD Type 2 columns
        current_plan_name,
        plan_start_date,
        dbt_valid_from                               as plan_effective_from,
        dbt_valid_to                                 as plan_effective_to,

        -- is_current: true if this is the latest record
        case
            when dbt_valid_to is null then true
            else false
        end                                          as is_current,

        -- SCD Type 3: previous plan name
        previous_plan_name

    from snapshot
)

select * from final