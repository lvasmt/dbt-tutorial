{{
  config(
    materialized = "table",
    partition_by={
      "field": "file_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["transport_type","route_name"]
  )
}}

with ptv_disruptions_with_stops as (
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
    from {{ref('stg_ptv_disruptions_stops_union')}}
)

select 
    * 
from ptv_disruptions_with_stops