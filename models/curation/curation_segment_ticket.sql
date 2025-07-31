{{ config(
    materialized='incremental',
    unique_key='dw_booking_id',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    partition_by={
        'field': 'booking_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['departure_city'],
) }}

{% if is_incremental() %}
    {% set query %}
        -- use source system's checkpoint and apply a 30-minute lookback window
        select max(coalesce(_fivetran_synced, _dbt_loaded_at)) - interval 30 minute as last_source_sync
        from {{ ref('stg_gds__booking') }}
    {% endset %}

    {% set result = run_query(query) %}
    {% set last_source_sync = result.columns[0].values()[0] %}
{% endif %}

with 
stg_gds__ticket as (
    select *
    from {{ ref('stg_gds__ticket') }}
    where not _fivetran_deleted
    {% if is_incremental() %}
        and coalesce(_fivetran_synced, _dbt_loaded_at) > '{{ last_source_sync }}'
    {% endif %}
),

stg_gds__booking as (
    select *
    from {{ ref('stg_gds__booking') }}
    where not _fivetran_deleted
    {% if is_incremental() %}
        and coalesce(_fivetran_synced, _dbt_loaded_at) > '{{ last_source_sync }}'
    {% endif %}
    qualify row_number() over (
        partition by booking_id
        order by coalesce(_fivetran_synced, _dbt_loaded_at) desc
    ) = 1
),

stg_gds__segment as (
    select *
    from {{ ref('stg_gds__segment') }}
    where not _fivetran_deleted
    {% if is_incremental() %}
        and coalesce(_fivetran_synced, _dbt_loaded_at) > '{{ last_source_sync }}'
    {% endif %}
),

stg_gds__ticket_segment as (
    select *
    from {{ ref('stg_gds__ticket_segment') }}
    where not _fivetran_deleted
    {% if is_incremental() %}
        and coalesce(_fivetran_synced, _dbt_loaded_at) > '{{ last_source_sync }}'
    {% endif %}
),

stg_gds__ticket_passenger as (
    select *
    from {{ ref('stg_gds__ticket_passenger') }}
    where not _fivetran_deleted
    {% if is_incremental() %}
        and coalesce(_fivetran_synced, _dbt_loaded_at) > '{{ last_source_sync }}'
    {% endif %}
),

segment_ticket_base as (
    select
        bk.booking_id
        , bk.passenger_id
        , tks.ticket_id
        , tks.segment_id

        -- booking
        , bk.booking_date
        , bk.booking_status
        , bk.booking_channel

        -- ticket
        , tk.ticket_number
        , tk.fare_class
        , tk.ticket_type
        , tk.base_price
        , tk.taxes
        , tk.fees
        , tk.total_price
        , tk.currency
        , tk.issue_date
        , tk.is_refundable
        , tk.is_changeable

        , tks.seat_number
        , tks.boarding_group
        , tks.checkin_status

        -- segment
        , sg.departure_city
        , sg.arrival_city
        , sg.departure_date
        , sg.departure_time
        , sg.arrival_date
        , sg.arrival_time
        , sg.airline
        , sg.flight_number
        , sg.aircraft
        , sg.duration_minutes

        -- passenger
        , tkp.is_primary
        , tkp.special_requests
        , tkp.miles_earned

    from stg_gds__booking bk
        inner join stg_gds__ticket tk on bk.booking_id = tk.booking_id
        inner join stg_gds__ticket_segment tks on tk.ticket_id = tks.ticket_id
        inner join stg_gds__segment sg on tks.segment_id = sg.segment_id
        left join stg_gds__ticket_passenger tkp on tk.ticket_id = tkp.ticket_id
),

segment_ticket as (
    select
        {{ dbt_utils.generate_surrogate_key(['ticket_id', 'segment_id']) }} as dw_booking_id
        
        -- dimensions
        , booking_id
        , passenger_id
        , ticket_id
        , segment_id
        , ticket_number
        , flight_number
        
        -- booking dates and routes
        , booking_date
        , issue_date
        , departure_date
        , departure_time
        , departure_city
        , arrival_date
        , arrival_time
        , arrival_city
        , duration_minutes

        -- measures 
        , base_price
        , taxes
        , fees
        , total_price
        , currency

        -- flight details
        , airline
        , aircraft
        , fare_class
        , ticket_type
        , miles_earned
        , booking_channel
        , booking_status
        , seat_number
        , checkin_status
        , boarding_group

        -- indicators
        , is_refundable
        , is_changeable
        , is_primary
        , special_requests is not null as has_special_requests
        , special_requests

        -- metrics
        , count(distinct passenger_id) over (partition by booking_id) as pax_per_booking
        , count(distinct ticket_id) over (partition by booking_id) as tickets_per_booking
        , count(distinct segment_id) over (partition by ticket_id) as segments_per_ticket
        , datediff('day', booking_date, departure_date) as booking_to_departure_days
        , case 
            when datediff('day', booking_date, departure_date) <= 1 then 'Last Minute'
            when datediff('day', booking_date, departure_date) <= 7 then 'Short Notice'  
            when datediff('day', booking_date, departure_date) <= 30 then 'Standard'
            else 'Advance'
        end as booking_window_category

    from segment_ticket_base
)
select 
    *

    -- metadata
    {% if is_incremental() %}
        , coalesce(
            (select _dw_created_at from {{ this }} where dw_booking_id = segment_ticket.dw_booking_id),
            now()   
        ) as _dw_created_at
    {% else %}
        , now() as _dw_created_at
    {% endif %}
    , now() as _dw_updated_at
    {% if is_incremental() %}
        , case
            when exists (select 1 from {{ this }} where dw_booking_id = segment_ticket.dw_booking_id)
                then 'UPDATE'
                else 'INSERT'
            end as _dbt_operation
    {% else %}
        , 'INSERT' as _dbt_operation
    {% endif %}
    , '{{ invocation_id }}' as _dbt_invocation_id
from segment_ticket