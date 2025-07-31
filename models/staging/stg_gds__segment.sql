select
    try_cast(segment_id as string) as segment_id
    , try_cast(booking_id as string) as booking_id
    , try_cast(segment_number as integer) as segment_number
    , try_cast(departure_city as string) as departure_city
    , try_cast(arrival_city as string) as arrival_city
    , try_cast(departure_date as date) as departure_date
    , try_cast(departure_time as time) as departure_time
    , try_cast(arrival_date as date) as arrival_date
    , try_cast(arrival_time as time) as arrival_time
    , try_cast(airline as string) as airline
    , try_cast(flight_number as string) as flight_number
    , try_cast(aircraft as string) as aircraft
    , try_cast(duration_minutes as integer) as duration_minutes
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'segment') }}