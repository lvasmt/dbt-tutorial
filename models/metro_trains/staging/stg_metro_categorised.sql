WITH unnested_trainlines AS (
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
        {{ref('stg_metro_unnested_train_lines')}}
),

issue_categorised AS (
    SELECT
        created_at,
        conversation_id,
        id,
        text,
        count(id) OVER (PARTITION BY conversation_id) AS thread_length
    FROM
        unnested_trainlines
    GROUP BY 
        created_at,
        conversation_id,
        id,
        text   
)



SELECT
    *
FROM
    issue_categorised
where id != conversation_id
ORDER BY 
    thread_length DESC,
    conversation_id ASC,
    created_at ASC
