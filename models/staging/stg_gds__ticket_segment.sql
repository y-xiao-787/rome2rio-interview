select
    try_cast(ticket_id as string) as ticket_id
    , try_cast(segment_id as string) as segment_id
    , try_cast(seat_number as string) as seat_number
    , try_cast(boarding_group as integer) as boarding_group
    , try_cast(checkin_status as string) as checkin_status
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'ticket_segment') }}