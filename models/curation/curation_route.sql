WITH 
stg_gds__segment as (
    select *
    from {{ ref('stg_gds__segment') }}
    where not _fivetran_deleted
),

route_base as (
    select distinct
        departure_city || '-' || arrival_city as route_code
        , departure_city
        , arrival_city
        -- region mapping
        , case 
            when departure_city in ('New York', 'Los Angeles', 'Chicago', 'Miami', 'San Francisco', 'Seattle', 'Boston', 'Denver') THEN 'North America'
            when departure_city in ('London', 'Paris', 'Rome', 'Barcelona', 'Amsterdam', 'Berlin', 'Prague', 'Vienna') THEN 'Europe'
            when departure_city in ('Tokyo', 'Singapore', 'Bangkok', 'Hong Kong', 'Seoul') THEN 'Asia'
            when departure_city in ('Sydney', 'Melbourne', 'Auckland') THEN 'Oceania'
            when departure_city in ('São Paulo', 'Buenos Aires', 'Mexico City') THEN 'Americas'
            else 'Other'
        end as departure_region
        , case 
            when arrival_city in ('New York', 'Los Angeles', 'Chicago', 'Miami', 'San Francisco', 'Seattle', 'Boston', 'Denver') THEN 'North America'
            when arrival_city in ('London', 'Paris', 'Rome', 'Barcelona', 'Amsterdam', 'Berlin', 'Prague', 'Vienna') THEN 'Europe'  
            when arrival_city in ('Tokyo', 'Singapore', 'Bangkok', 'Hong Kong', 'Seoul') THEN 'Asia'
            when arrival_city in ('Sydney', 'Melbourne', 'Auckland') THEN 'Oceania'
            when arrival_city in ('São Paulo', 'Buenos Aires', 'Mexico City') THEN 'Americas'
            else 'Other'
        end as arrival_region
        , count(distinct segment_id) as segment_count
        , count(distinct case when date_part('month', departure_date) in (12, 1, 2) then segment_id end) as winter_flight_count
        , count(distinct case when date_part('month', departure_date) in (3, 4, 5) then segment_id end) as spring_flight_count
        , count(distinct case when date_part('month', departure_date) in (6, 7, 8) then segment_id end) as summer_flight_count
        , count(distinct case when date_part('month', departure_date) in (9, 10, 11) then segment_id end) as autumn_flight_count
        , count(distinct booking_id) as total_booking_count
        , count(distinct airline) as airline_count
        , count(distinct flight_number) as airline_count
        , avg(duration_minutes) as avg_duration_minutes
        , min(date(departure_date)) as first_service_date
        , max(date(departure_date)) as last_service_dat

    from stg_gds__segment
    group by all
),

route_metrics as (
    select
        *
        , case 
            when departure_region = arrival_region then 'Domestic'
            else 'International'
        end as route_type
        , case 
            when avg_duration_minutes <= 180 then 'Short Haul'
            when avg_duration_minutes <= 420 then 'Medium Haul'  
            else 'Long Haul'
        end as distance_category
        , row_number() over (order by total_booking_count desc) as popularity_rank
        , greatest(winter_flight_count, spring_flight_count, summer_flight_count, autumn_flight_count) > segment_count * 0.4 as is_seasonal_route
        , case
            when winter_flight_count = greatest(winter_flight_count, spring_flight_count, summer_flight_count, autumn_flight_count) then 'Winter'
            when spring_flight_count = greatest(winter_flight_count, spring_flight_count, summer_flight_count, autumn_flight_count) then 'Spring'
            when summer_flight_count = greatest(winter_flight_count, spring_flight_count, summer_flight_count, autumn_flight_count) then 'Summer'
            when autumn_flight_count = greatest(winter_flight_count, spring_flight_count, summer_flight_count, autumn_flight_count) then 'Autumn'
            else 'Year Round'
        end as peak_season
    from route_base
)

select 
    *
    -- metadata
    , now() as _dbt_created_at
    , now() as _dbt_updated_at
    , '{{ invocation_id }}' as _dbt_invocation_id
from route_metrics