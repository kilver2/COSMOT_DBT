with breweries as (

    select *
    from {{ ref('int__brewery__breweries') }}

),

state_breweries as (

    select 
        count(*) as brewery_count, 
        state, 
        brewery_type
    
    from breweries

    group by 
        state, 
        brewery_type

),

state_totals as (

    select
        count(*) as total_breweries,
        state
    
    from breweries

    group by state

),

ranked_states as (

    select
        state,
        total_breweries,

        dense_rank() over (
            order by total_breweries desc
        ) as state_rank

    from state_totals

)

select 
    sb.state,
    rs.state_rank,
    sb.brewery_type,
    sb.brewery_count,
    rs.total_breweries

from state_breweries as sb

join ranked_states as rs
    on sb.state = rs.state

where rs.state_rank <= 10

order by
    rs.state_rank,
    sb.brewery_count desc