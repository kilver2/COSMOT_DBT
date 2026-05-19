with breweries as (

    select *
    from {{ ref('stg__brewery__breweries') }}

),

deduped as (

    select *
    from breweries

    qualify row_number() over (
        partition by brewery_id
        order by _ingested_at desc
    ) = 1

)

select *
from deduped
