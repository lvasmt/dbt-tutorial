WITH stg_data AS (
    SELECT 
        * 
    FROM 
        {{source('metro_data','stg_metro_trains_twitter')}}
), 

extracted_train_lines AS (
    SELECT  
        time_of_load,
        id, 
        conversation_id,
        created_date, 
        created_time,
        created_hour, 
        info_type,
        text,
        REGEXP_EXTRACT_ALL(text,'Frankston|Pakenham|Cranbourne|Sandringham|Craigieburn|Upfield|Sydenham|Williamstown|W.town|Werribee|Lilydale|Glen Waverley|Belgrave|Alamein|Hurstbridge|Epping|Stony Point|Melton|Sunbury') as train_lines,
        info_on_issue,
        reason
    FROM 
        stg_data
),

unnested_train_lines AS (
    SELECT 
        time_of_load,
        id, 
        conversation_id,
        created_date, 
        created_time,
        created_hour, 
        info_type,
        text,
        info_on_issue,
        reason,
        train_line
    FROM    
        extracted_train_lines as etr
    CROSS JOIN UNNEST(etr.train_lines) AS train_line
),

table_with_dedupe_key AS (
    SELECT 
        *,
        SHA256(CONCAT(CAST(conversation_id AS STRING),train_line)) AS dedupe_key
    FROM
        unnested_train_lines
),

numbered_rows AS (
    SELECT  
        *,
        ROW_NUMBER() OVER (PARTITION BY dedupe_key ORDER BY time_of_load DESC,created_time ASC) as row_num
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
)


SELECT  
    *
FROM    
    deduplicated_rows