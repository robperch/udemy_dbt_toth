with mfm as (
    select
        *
    from
        {{ ref('mart_full_moon_reviews') }}
)

select
    is_full_moon,
    review_sentiment,
    count(*) as reviews
from
    mfm
group by
    is_full_moon,
    review_sentiment
order by
    is_full_moon,
    review_sentiment
