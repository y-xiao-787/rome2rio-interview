select
    try_cast(booking_id as string) as booking_id
    , try_cast(customer_id as string) as passenger_id
    , try_cast(booking_date as date) as booking_date
    , try_cast(booking_status as string) as booking_status
    , try_cast(total_amount as numeric) as total_amount
    , try_cast(currency as string) as currency
    , try_cast(booking_channel as string) as booking_channel
    , try_cast(created_at as timestamp) as created_at
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'booking') }}