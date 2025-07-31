select
    try_cast(ticket_id as string) as ticket_id
    , try_cast(passenger_id as string) as passenger_id
    , try_cast(is_primary as boolean) as is_primary
    , try_cast(special_requests as string) as special_requests
    , try_cast(loyalty_points as integer) as loyalty_points
    , try_cast(miles_earned as integer) as miles_earned
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'ticket_passenger') }}