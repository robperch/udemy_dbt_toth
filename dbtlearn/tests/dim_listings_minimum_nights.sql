-- Test to check if the minimum number of nights on any listing is less than 1
select
    *
from
    {{ ref('dim_listings_cleansed') }}
where
    minimum_nights < 1
limit 10