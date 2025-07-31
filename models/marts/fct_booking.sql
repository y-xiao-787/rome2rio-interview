with 
curation_segment_ticket as (
    select *
    from {{ ref('curation_segment_ticket') }}
)

select
    -- foreign keys
    {{ dbt_utils.generate_surrogate_key(['passenger_id']) }} as dw_passenger_id
    , {{ dbt_utils.generate_surrogate_key(['departure_city', 'arrival_city']) }} as dw_route_id
    
    , *
from curation_segment_ticket