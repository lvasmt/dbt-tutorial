WITH unnested_train_lines AS (
    SELECT 
        time_of_load,
        conversation_id,
        id,
        created_at,
        ts_created_at,
        aest_created_day_of_week,
        aest_created_date,
        aest_created_time,
        text,
        external_tagging,
        info_type,
        train_line,
        info_on_issue,
        reason
    FROM    
        {{ref('stg_metro_extracted_info')}} as extracted_info
    CROSS JOIN UNNEST(extracted_info.train_lines) AS train_line
),

--This section deduplicates the data so that we have a row for each line/tweet id combination.
table_with_dedupe_key AS (
    SELECT 
        *,
        SHA256(CONCAT(CAST(id AS STRING),train_line)) AS dedupe_key
    FROM
        unnested_train_lines
),

numbered_rows AS (
    SELECT  
        *,
        ROW_NUMBER() OVER (PARTITION BY dedupe_key ORDER BY time_of_load DESC,ts_created_at ASC) as row_num
    FROM    
        table_with_dedupe_key
),

deduplicated_rows AS (
    SELECT  
        *
    FROM    
        numbered_rows
    WHERE
        row_num=1
),

fixed_train_names as (
    SELECT 
        time_of_load,
        conversation_id,
        id,
        created_at,
        ts_created_at,
        aest_created_day_of_week,
        aest_created_date,
        aest_created_time,
        text,
        external_tagging,
        info_type,
        case 
            when train_line = 'W\'town' then 'Williamstown'
            else train_line
        end as train_line,
        info_on_issue,
        reason
    FROM    
        deduplicated_rows
)

SELECT
    *
FROM
    fixed_train_names
