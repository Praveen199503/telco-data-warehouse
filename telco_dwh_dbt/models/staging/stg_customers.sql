-- ============================================================
-- stg_customers.sql
-- Light cleanup of raw customer data from staging
-- Materialized as: VIEW (no storage, always fresh)
-- ============================================================

with source as (
    select * from {{ source('staging', 'stg_customers') }}
),

cleaned as (
    select
        customer_id,

        -- Trim whitespace from names
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,

        -- Lowercase email for consistency
        lower(trim(email))                          as email,

        phone_number,
        date_of_birth,

        -- Standardise gender
        initcap(gender)                             as gender,

        trim(city)                                  as city,
        trim(country)                               as country,
        registration_date,

        -- Standardise status to lowercase
        lower(customer_status)                      as customer_status,

        -- Derived: age group (useful for segmentation)
        case
            when date_part('year', age(date_of_birth)) between 18 and 25 then '18-25'
            when date_part('year', age(date_of_birth)) between 26 and 35 then '26-35'
            when date_part('year', age(date_of_birth)) between 36 and 50 then '36-50'
            else '50+'
        end                                         as age_group,

        -- Metadata
        _loaded_at,
        _is_valid

    from source
    where customer_id is not null    -- Hard filter: no orphan records
)

select * from cleaned