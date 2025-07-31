with
curation_passenger as (
    select *
    from {{ ref('curation_passenger') }}
)

select *
from curation_passenger