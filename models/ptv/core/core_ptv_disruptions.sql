{{
  config(
    materialized = "table",
    partition_by={
      "field": "file_date",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH final_table AS (
    SELECT
        file_date, 
        disruption_id, 
        title,
        url,
        description,
        disruption_status, 
        disruption_type,
        published_on,
        last_updated,
        from_date,
        to_date,
        transport_type,
        info_on_issue,
        reason
    FROM
        {{ref('stg_ptv_categorised')}}
)

SELECT 
    * 
FROM 
    final_table