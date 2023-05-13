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
    REGEXP_EXTRACT(text,r'(?i)^([\W]).*') as info_type,
    REGEXP_EXTRACT_ALL(text,r'All lines|Frankston|Pakenham|Cranbourne|Sandringham|Craigieburn|Upfield|Sydenham|Williamstown|W.town|Werribee|Lilydale|Glen Waverley|Belgrave|Alamein|Hurstbridge|Epping|Stony Point|Melton|Sunbury|Mernda|Bell ') as train_lines,
    CASE
        WHEN REGEXP_CONTAINS(text,r'(?i).*buses replace trains.*|.*replacement buses.*|.*shuttle service.*|.*buses.*replac.*') THEN "Bus Replacements"
        WHEN REGEXP_CONTAINS(text,r'(?i).*clearing.*') THEN "Delays Clearing"
        WHEN REGEXP_CONTAINS(text,r'(?i).*major delays.*') THEN "Major Delays"
        WHEN REGEXP_CONTAINS(text,r'(?i).*delays.*') THEN "Delays"
        WHEN REGEXP_CONTAINS(text,r'(?i).*enter.*via.*') THEN "Different Station Entry"
        WHEN REGEXP_CONTAINS(text,r'(?i).*altered timetable.*') THEN "Altered Timetable"
        WHEN REGEXP_CONTAINS(text,r'(?i).*no.*lift.*|.*no.*escalator.*|.*lifts.*not in service.*') THEN "No lift/escalator access"
        WHEN REGEXP_CONTAINS(text,r'(?i).*direct to.*flinder.*|.*not?.*city loop.*') THEN "No City Loop / Direct to Flinders"
        WHEN REGEXP_CONTAINS(text,r'(?i).*trains will not stop .*station.*') THEN "Trains not stopping at specific stations"
        WHEN REGEXP_CONTAINS(text,r'(?i).*reduced service frequency.*') THEN "Reduced service frequency"

        ELSE NULL 
    END AS info_on_issue,
    CASE
        WHEN REGEXP_CONTAINS(text,r'(?i).*level.?crossing.*|.*project.*') then "Level Crossing Works"
        WHEN REGEXP_CONTAINS(text,r'(?i).*works.*|.*maintenance.*') then "Maintenance & Other Works"
        WHEN REGEXP_CONTAINS(text,r'(?i).*outage.*') then "Power Outage"
        WHEN REGEXP_CONTAINS(text,r'(?i).*trespasser.*') then "Trespasser"
        WHEN REGEXP_CONTAINS(text,r'(?i).*fault.*|.*overhead.*|.*signalling systems.*|.*train.*removed from service.*') then "Equipment Fault"
        WHEN REGEXP_CONTAINS(text,r'(?i).*police.*|.*vicpol.*') then "Police Request"
        WHEN REGEXP_CONTAINS(text,r'(?i).*ill passenger.*|.*medical.*assistance.*') then "Sick Passenger"
        WHEN REGEXP_CONTAINS(text,r'(?i).*animal.*|.*dog.*|.*kangaroo.*') then "Animals On Train Tracks"
        WHEN REGEXP_CONTAINS(text,r'(?i).*person.*hit.*|.*hit.*person|.*str..?k.*person.*|.*person.*struck.*by.*train.*') then "Person Hit By Train"
        WHEN REGEXP_CONTAINS(text,r'(?i).*operation.*incident.*|.*operation.*issue.*|.*drivers.*trains.*out of position.*') then "Operational Incident"
        WHEN REGEXP_CONTAINS(text,r'(?i).*operation.*request.*|.*operational requirement.*') then "Operational Request"
        WHEN REGEXP_CONTAINS(text,r'(?i).*service disruption|earlier disruption.*') then "Service Disruption"
        WHEN REGEXP_CONTAINS(text,r'(?i).*vandal.*') then "Vandalism"
        WHEN REGEXP_CONTAINS(text,r'(?i).*disruptive passenger.*|.*unruly.*') then "Disruptive Passenger"
        WHEN REGEXP_CONTAINS(text,r'(?i).*flood.*') then "Flooding"
        WHEN REGEXP_CONTAINS(text,r'(?i).*hit.*bridge.*|.*truck.*hit.*bridge.*|.*str..?k.*bridge.*|.*bridge.*str..?k|.*bridge.*hit.*vehicle.*') then "Bridge Hit By Vehicle"
        WHEN REGEXP_CONTAINS(text,r'(?i).*extreme.*weather.*|.*storm.*') then "Storm Damage"
        WHEN REGEXP_CONTAINS(text,r'(?i).*weather.*') then "Weather related issues"
        WHEN REGEXP_CONTAINS(text,r'(?i).* tree.*') then "Trees On Tracks"
        WHEN REGEXP_CONTAINS(text,r'(?i).* debris.*') then "Debris On Tracks"
        WHEN REGEXP_CONTAINS(text,r'(?i).*vehicle.*block.*|.*car.*block.*|.*car.*stop.*|.*vehicle.*across.*|.*vehicle.*on.*|.*near hit.*') then "Vehicle Across Tracks"
        WHEN REGEXP_CONTAINS(text,r'(?i).*vehicle.*hit.*by.*train.*|vehicle.*struck.*by.*train.*|.*[car|bus|vehicle|truck|motor].*hit.*by.*train.*') then "Vehicle Hit By Train"
        WHEN REGEXP_CONTAINS(text,r'(?i).*frv.*|.*emergency.*|.*fire.*|.*ambulance.*|.*police.*|.*vicpol.*|.*cfa request.*') then "Emergency Services Request"
        WHEN REGEXP_CONTAINS(text,r'(?i).*rally.*') then "Rally"
        WHEN REGEXP_CONTAINS(text,r'(?i).*congestion.*') then "Congestion"
        WHEN REGEXP_CONTAINS(text,r'(?i).*game.*|.*concert.*|.*#AFL.*|.*match.*|.*day out.*|.*flemington racecourse.*|.*cricket.*') then "Event in the city"
        ELSE TRIM(REGEXP_EXTRACT(text,r'due to [^\n]*|while[^\n]*|after [^\n]*')) 
    END AS reason
  FROM
    {{ref('stg_metro_datetime_extraction')}}
)


SELECT
  *
FROM
  extracted_info