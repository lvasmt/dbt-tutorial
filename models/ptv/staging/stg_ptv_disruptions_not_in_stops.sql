with disruptions_in_stops as (
    select distinct 
        disruption_id
    from 
        {{source('ptv_data','raw_ptv_disruptions_stops')}}
), 

disruptions as (
    select 
        d.disruption_id, 
        d.route_id, 
        d.file_date,
        d.route_type,
        d.route_number,
        d.from_date,
        d.last_updated,
        d.published_on,
        d.to_date,
        d.title,
        d.description,
        d.disruption_status,
        d.disruption_type,
        d.transport_type,
        d.info_on_issue,
        d.reason
    from 
        {{ref('stg_ptv_disruptions_with_routes')}} as d 
        left join disruptions_in_stops as dis 
            on CAST(d.disruption_id AS STRING) = CAST(dis.disruption_id AS STRING)
    where 
        dis.disruption_id is null
), 

stops_data as (
    select distinct
        load_date,
        route_id,
        route_name,
        route_type_name,
        stop_suburb,
        stop_latitude,
        stop_longitude,
        stop_id,
        stop_name
    from 
        {{source('ptv_data','raw_ptv_stops')}}
    --having load_date = max(load_date) over (partition by stop_id) 
    -- The above only to be enabled in case more data is loaded later on as it is computationally taxing. 
),

disruptions_with_stops as (
    select 
        -- disruption info         
        d.disruption_id, 
        d.title,
        d.description,
        d.disruption_status,
        d.disruption_type,
        d.transport_type,
        d.info_on_issue,
        d.reason,

        -- date info 
        d.file_date,
        d.from_date,
        d.last_updated,
        d.published_on,
        d.to_date,
        -- route information 
        d.route_id, 
        d.route_type,
        d.route_number,
        sd.route_type_name, 
        sd.route_name, 

        -- stops id 
        sd.stop_id, 
        sd.stop_name,
        sd.stop_suburb,
        sd.stop_latitude,
        sd.stop_longitude,
        CONCAT(sd.stop_latitude,',',sd.stop_longitude) as stop_geo_location
    from 
        disruptions as d 
        left join stops_data as sd 
            on CAST(d.route_id AS STRING) = CAST(sd.route_id AS STRING)
)


select 
    * 
from disruptions_with_stops 

