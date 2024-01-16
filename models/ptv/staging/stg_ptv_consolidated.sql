WITH selected_cols AS(
    SELECT
        date,
        PARSE_TIMESTAMP('%Y%m%d',LEFT(file_date,8)) AS file_date,
        PARSE_TIME('%H%M',right(file_date,4)) AS file_time,
        disruption_id, 
        title,
        url,
        description,
        disruption_status, 
        disruption_type,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',published_on) AS published_on,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',last_updated) AS last_updated,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',from_date) AS from_date,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',to_date) AS to_date,
        transport_type
    FROM 
        {{source('ptv_data','raw_ptv_disruptions')}}
),

consolidated AS (
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
        transport_type
    FROM 
        selected_cols
    GROUP BY
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
        transport_type
)

SELECT 
    * 
FROM consolidated



