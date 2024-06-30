-- Invoking dim base tables
with dim_hosts_cleansed as (
    select
        *
    from
        {{ ref('dim_hosts_cleansed') }}
),
dim_listings_cleansed as (
    select
        *
    from
        {{ ref('dim_listings_cleansed') }}
)

-- Joining both dim base tables
select
    -- dim_host_cleansed columns
    dhc.host_id,
    dhc.host_name,
    dhc.is_superhost as host_is_superhost,
    dhc.created_at as dhc_created_at,
    -- dim_listing_cleansed columns
    dlc.listing_id,
    dlc.listing_name,
    dlc.listing_url,
    dlc.room_type,
    dlc.minimum_nights,
    dlc.price,
    dlc.created_at as dlc_created_at,
    -- Selection from both tables
    greatest(dhc.updated_at, dlc.updated_at) as updated_at
from
    dim_hosts_cleansed as dhc
left join
    dim_listings_cleansed dlc on dhc.host_id = dlc.host_id