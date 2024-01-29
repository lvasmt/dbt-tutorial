with disruptions_with_stops_specified as (
    select 
        --disruption info
        disruption_id,
        title,
        description,
        disruption_status,
        disruption_type,
        transport_type,
        info_on_issue,
        reason,

        -- date info 
        from_date,
        last_updated,
        published_on,
        to_date,
        file_date,

        -- route info 
        route_id,
        route_type,
        route_name,
        route_number,
        route_type_name,

        -- stop info
        stop_id,
        stop_name,
        stop_suburb,
        stop_latitude,
        stop_longitude,
        stop_geo_location
    from 
        {{ref('stg_ptv_stops_disruptions')}}
), 

disruptions_with_stops_not_specified as (
    select 
        --disruption info
        disruption_id,
        title,
        description,
        disruption_status,
        disruption_type,
        transport_type,
        info_on_issue,
        reason,

        -- date info 
        from_date,
        last_updated,
        published_on,
        to_date,
        file_date,

        -- route info 
        route_id,
        route_type,
        route_name,
        route_number,
        route_type_name,

        -- stop info
        stop_id,
        stop_name,
        stop_suburb,
        stop_latitude,
        stop_longitude,
        stop_geo_location
    from 
        {{ref('stg_ptv_disruptions_not_in_stops')}}
), 

unioned as (
    select 
        *
    from disruptions_with_stops_specified
    union all 
    select 
        * 
    from disruptions_with_stops_not_specified
)

select distinct 
    * 
from unioned 