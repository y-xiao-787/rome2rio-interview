with
curation_route as (
    select *
    from {{ ref('curation_route') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['departure_city', 'arrival_city']) }} as dw_route_id
    , *
from curation_route