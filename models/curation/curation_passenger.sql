with 
snp_gds__passenger as (
    select * 
    from {{ ref('snp_gds__passenger') }}
),

stg_gds__booking as (
    select *
    from {{ ref('stg_gds__booking') }}
    where not _fivetran_deleted
    qualify row_number() over (
        partition by booking_id
        order by coalesce(_fivetran_synced, _dbt_loaded_at) desc
    ) = 1
),

customer_bookings as (
    select
        ps.passenger_id
        , ps._dbt_valid_from
        , ps._dbt_valid_to
        , count(distinct 
            case 
                when bk.booking_date < ps._dbt_valid_to
                then bk.booking_id
            end
        ) as booking_count
        , min(
            case 
                when bk.booking_date < ps._dbt_valid_to
                then bk.booking_date 
            end
        ) as first_booking_date
        , max(
            case 
                when bk.booking_date < ps._dbt_valid_to
                then bk.booking_date 
            end
        ) as last_booking_date
        , sum(
            case 
                when bk.booking_date < ps._dbt_valid_to
                then bk.total_amount
                else 0 
            end
        ) as lifetime_value
        , avg(case
            when bk.booking_date < ps._dbt_valid_to
            then bk.total_amount
        end) as avg_booking_value
    from snp_gds__passenger ps
        left join stg_gds__booking bk on ps.passenger_id = bk.passenger_id
    group by all
)

select
    {{ dbt_utils.generate_surrogate_key(['ps.passenger_id']) }} as dw_passenger_id
    , ps.passenger_id
    , ps.booking_id
    , ps.first_name
    , ps.last_name
    , concat(ps.first_name, ' ', ps.last_name) as full_name
    , ps.email
    , split_part(ps.email, '@', 2) as email_domain
    , ps.phone
    , ps.date_of_birth
    , date_diff('year', current_date, ps.date_of_birth) as age
    , case 
        when date_diff('year', current_date, ps.date_of_birth) < 2 then 'Infant'
        when date_diff('year', current_date, ps.date_of_birth) < 12 then 'Child'
        when date_diff('year', current_date, ps.date_of_birth) < 18 then 'Minor'
        when date_diff('year', current_date, ps.date_of_birth) between 18 and 24 then 'Young Adult'
        when date_diff('year', current_date, ps.date_of_birth) between 25 and 34 then 'Adult'
        when date_diff('year', current_date, ps.date_of_birth) between 35 and 54 then 'Middle Age'
        when date_diff('year', current_date, ps.date_of_birth) >= 55 then 'Senior'
        else 'Unknown'
    end as age_group
    , ps.gender
    , ps.nationality
    , ps.passenger_type = 'adult' as is_adult
    , ps.passenger_type
    , ps.frequent_flyer_number is not null as has_frequent_flyer
    , ps.frequent_flyer_number

    , coalesce(cb.booking_count, 0) as total_bookings
    , cb.first_booking_date
    , cb.last_booking_date
    , coalesce(cb.lifetime_value, 0) as lifetime_value
    , cb.avg_booking_value
    
    -- metadata
    , ps._dbt_valid_from
    , ps._dbt_valid_to
    , ps._dbt_valid_to = '9999-12-31 23:59:59' as is_current
    , ps._dbt_updated_at
    , ps._dbt_scd_id
from snp_gds__passenger ps
    left join customer_bookings cb on (
        ps.passenger_id = cb.passenger_id 
        and ps._dbt_valid_from = cb._dbt_valid_from
        and ps._dbt_valid_to = cb._dbt_valid_to
    )