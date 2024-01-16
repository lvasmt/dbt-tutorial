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

WITH final_table AS (
    SELECT
        disruption_id,
        route_name,
        route_type,
        route_number,
        file_date,
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
    FROM
        {{ref('stg_ptv_disruptions_with_routes')}}
)

SELECT 
    * 
FROM 
    final_table