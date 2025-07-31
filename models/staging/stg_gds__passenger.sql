select
    try_cast(passenger_id as string) as passenger_id
    , try_cast(booking_id as string) as booking_id
    , try_cast(first_name as string) as first_name
    , try_cast(last_name as string) as last_name
    , try_cast(email as string) as email
    , try_cast(phone as string) as phone
    , try_cast(date_of_birth as date) as date_of_birth
    , try_cast(gender as string) as gender
    , try_cast(nationality as string) as nationality
    , try_cast(passenger_type as string) as passenger_type
    , try_cast(frequent_flyer_number as string) as frequent_flyer_number
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'passenger') }}