-- Ensuring that there is no review created before the listing creation
select
    *
from
    {{ ref('fct_reviews') }} as r
left join
    {{ ref('dim_listings_cleansed') }} l on r.listing_id = l.listing_id
where
    r.review_date < l.created_at