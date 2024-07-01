-- Initial configuration of the model
{{
    config(
        materialized = 'table',
    )
}}

-- Invoking models as CTE's
with seed_full_moon_dates as (
    select
        *
    from
        {{ ref('seed_full_moon_dates') }}
),
fct_reviews as (
    select
        *
    from
        {{ ref('fct_reviews')}}
)

-- Joining the two models
select
    fr.*,
    case
        when sf.full_moon_date is null then 'not full moon'
        else 'full moon'
    end as is_full_moon
from
    fct_reviews fr
left join
    seed_full_moon_dates sf on dateadd(day, 1, sf.full_moon_date) = to_date(fr.review_date)