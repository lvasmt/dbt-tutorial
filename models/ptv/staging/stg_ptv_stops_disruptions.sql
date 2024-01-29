WITH stops_disruptions as (
    select distinct
        disruption_id,
        stop_id
    from 
        {{source('ptv_data','raw_ptv_disruptions_stops')}}
),

stops_data as (
    select
        load_date,
        stop_suburb,
        stop_latitude,
        stop_longitude,
        stop_id,
        stop_name,
        route_id,
        route_name,
        route_type,
        route_type_name
    from 
        {{source('ptv_data','raw_ptv_stops')}}
    --having load_date = max(load_date) over (partition by stop_id) 
    -- The above only to be enabled in case more data is loaded later on as it is computationally taxing. 
),

disruptions as (
    select 
        disruption_id, 
        route_id, 
        file_date,
        route_type,
        route_number,
        from_date,
        last_updated,
        published_on,
        to_date,
        title,
        description,
        disruption_status,
        disruption_type,
        transport_type,
        info_on_issue,
        reason
    from 
        {{ref('stg_ptv_disruptions_with_routes')}}
        
),

stops_with_disruptions_info as(
    select 
        s_dis.disruption_id,
        cast(s_dis.stop_id as int) as stop_id,
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
        stops_disruptions as s_dis --stops_id, disruption_id
        left join disruptions as d --disruption_id, route_id
            on cast(d.disruption_id AS INT) = CAST(s_dis.disruption_id AS INT)
), 

disruptions_with_stops_data as (
    select 
        --disruption info
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
        

        -- route info 
        ifnull(d.route_id,cast(s.route_id as int)) as route_id,
        d.route_number,
        ifnull(d.route_type,cast(s.route_type as int)) as route_type,
        s.route_name,
        s.route_type_name,

        -- stop info
        d.stop_id,
        s.stop_name,
        s.stop_suburb,
        s.stop_latitude,
        s.stop_longitude,
        CONCAT(s.stop_latitude,',',s.stop_longitude) as stop_geo_location
    from 
        stops_with_disruptions_info as d 
        left join stops_data as s 
            on (cast(d.stop_id as int) = cast(s.stop_id as int)
                and cast(d.route_id as int) = cast(s.route_id as int)) 
            OR (cast(d.stop_id as int) = cast(s.stop_id as int) and d.route_id is null)

)

select 
    * 
from 
    disruptions_with_stops_data