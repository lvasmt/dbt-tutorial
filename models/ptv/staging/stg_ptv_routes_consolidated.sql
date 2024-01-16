WITH selected_cols AS (
    SELECT 
        PARSE_TIMESTAMP('%Y%m%d',LEFT(file_date,8)) AS file_date,
        disruption_id,
        route_type,
        route_id,
        route_name,
        route_number,
        route_gtfs_id
    FROM 
        {{source('ptv_data','raw_ptv_disruptions_routes')}}
), 
consolidated AS (
    SELECT 
        file_date,
        disruption_id,
        route_type,
        route_id,
        route_name,
        route_number,
        route_gtfs_id
    FROM 
        selected_cols
    GROUP BY
        file_date,
        disruption_id,
        route_type,
        route_id,
        route_name,
        route_number,
        route_gtfs_id
)

SELECT 
    *
FROM 
    consolidated