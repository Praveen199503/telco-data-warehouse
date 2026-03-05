-- ============================================================
-- stg_billing_events.sql
-- ============================================================

with source as (
    select * from {{ source('staging', 'stg_billing_events') }}
),

cleaned as (
    select
        billing_id,
        customer_id,
        subscription_id,
        billing_date,
        billing_period_start,
        billing_period_end,

        greatest(amount_due, 0)                     as amount_due,
        greatest(amount_paid, 0)                    as amount_paid,

        -- Derived: outstanding balance
        greatest(amount_due - amount_paid, 0)       as outstanding_balance,

        payment_date,
        lower(payment_status)                       as payment_status,
        payment_method,

        -- Derived: days taken to pay (null if not paid yet)
        case
            when payment_date is not null
            then payment_date - billing_date
            else null
        end                                         as days_to_payment,

        -- Derived: is this overdue?
        case
            when payment_status = 'overdue' then true
            when payment_date is null
             and billing_date < current_date - interval '30 days' then true
            else false
        end                                         as is_overdue,

        _loaded_at

    from source
    where billing_id is not null
)

select * from cleaned