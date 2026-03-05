-- ============================================================
-- dim_date.sql
-- Covers full year 2024
-- ============================================================

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2024-01-01' as date)",
            end_date="cast('2024-12-31' as date)"
        )
    }}
),

final as (
    select
        -- Surrogate key: integer format YYYYMMDD
        cast(to_char(date_day, 'YYYYMMDD') as integer)  as date_key,

        date_day                                         as full_date,
        to_char(date_day, 'Day')                         as day_of_week,
        extract(day from date_day)::integer              as day_number,
        extract(month from date_day)::integer            as month_number,
        to_char(date_day, 'Month')                       as month_name,
        'Q' || extract(quarter from date_day)::text      as quarter,
        extract(year from date_day)::integer             as year,

        -- Is it a weekend?
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                              as is_weekend,

        false                                            as is_holiday

    from date_spine
)

select * from final