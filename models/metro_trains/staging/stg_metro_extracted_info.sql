WITH extracted_info AS (
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
    REGEXP_EXTRACT(text,r'\@[^\s]*') AS external_tagging,
    REGEXP_EXTRACT(text,r'(?i)([\W]).*[Frankston|Pakenham|Cranbourne|Sandringham|Craigieburn|Upfield|Sydenham|Williamstown|W.town|Werribee|Lilydale|Glen Waverley|Belgrave|Alamein|Hurstbridge|Epping|Stony Point|Melton|Sunbury].*line') as info_type,
    REGEXP_EXTRACT_ALL(text,r'Frankston|Pakenham|Cranbourne|Sandringham|Craigieburn|Upfield|Sydenham|Williamstown|W.town|Werribee|Lilydale|Glen Waverley|Belgrave|Alamein|Hurstbridge|Epping|Stony Point|Melton|Sunbury') as train_lines,
    TRIM(REGEXP_EXTRACT(text,r'(?i).*[lines]{4,5}\:(.*)(?:due.*|while.*)')) AS info_on_issue,
    TRIM(REGEXP_EXTRACT(text,r'due to [^\n]*|while[^\n]*')) AS reason
  FROM
    {{ref('stg_metro_datetime_extraction')}}
)


SELECT
  *
FROM
  extracted_info