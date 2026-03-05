-- ============================================================
-- dim_customer_snapshot.sql
-- SCD Type 2: tracks full history of customer plan changes
-- ============================================================

{% snapshot dim_customer_snapshot %}

{{
    config(
        target_schema='dwh',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_status',
            'city',
            'email',
            'current_plan_name'
        ],
        invalidate_hard_deletes=True
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
    where is_active = true
),

plans as (
    select * from {{ ref('stg_service_plans') }}
),

-- Join to get current plan name for each customer
customer_with_plan as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.gender,
        c.city,
        c.country,
        c.age_group,
        c.registration_date,
        c.customer_status,

        -- SCD Type 1 fields (just overwrite on change)
        -- email and city are included in check_cols above
        -- DBT will overwrite these automatically

        -- SCD Type 2: current plan (tracked via snapshot rows)
        p.plan_name                                  as current_plan_name,
        s.start_date                                 as plan_start_date,

        -- SCD Type 3: previous plan stored as a column
        -- We use lag() to get the previous plan
        lag(p.plan_name) over (
            partition by c.customer_id
            order by s.start_date
        )                                            as previous_plan_name,

        -- Customer segment derived from status + plan tier
        case
            when c.customer_status = 'churned'       then 'Churned'
            when p.plan_tier = 'Premium'             then 'High Value'
            when p.plan_tier = 'Standard'            then 'Medium Value'
            else                                          'Low Value'
        end                                          as customer_segment

    from customers c
    left join subscriptions s
        on c.customer_id = s.customer_id
    left join plans p
        on s.plan_id = p.plan_id
)

select * from customer_with_plan

{% endsnapshot %}