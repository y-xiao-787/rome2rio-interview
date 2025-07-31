select
    try_cast(ticket_id as string) as ticket_id
    , try_cast(booking_id as string) as booking_id
    , try_cast(ticket_number as string) as ticket_number
    , try_cast(fare_class as string) as fare_class
    , try_cast(ticket_type as string) as ticket_type
    , try_cast(base_price as numeric) as base_price
    , try_cast(taxes as numeric) as taxes
    , try_cast(fees as numeric) as fees
    , try_cast(total_price as numeric) as total_price
    , try_cast(currency as string) as currency
    , try_cast(issue_date as date) as issue_date
    , try_cast(valid_until as date) as valid_until
    , try_cast(is_refundable as boolean) as is_refundable
    , try_cast(is_changeable as boolean) as is_changeable
    , _fivetran_deleted
    , _fivetran_synced
    , now() as _dbt_loaded_at
from {{ source('gds', 'ticket') }}