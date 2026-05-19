with source as (

    select *
    from {{ source('bronze', 'raw_breweries') }}
    where country = 'United States'

),

staging as (

    select
        cast(id as string) as brewery_id,
        brewery_type,
        postal_code,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        _ingested_at,
        lower(trim(name)) as brewery_name,
        lower(address_1) as brewery_address,
        lower(city) as city,
        lower(state_province) as `state`,
        current_timestamp() as transformed_at

    from source

)

select
    brewery_id,
    brewery_name,
    brewery_type,
    city,
    `state`,
    postal_code,
    latitude,
    longitude,
    _ingested_at,
    transformed_at
from staging
