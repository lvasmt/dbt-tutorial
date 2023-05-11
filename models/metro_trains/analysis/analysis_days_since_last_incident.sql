with raw as (
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
  FROM {{ref('core_metro_trains')}} 
), 

event_dates as (
  select
    aest_created_date,
    reason,
    info_on_issue,
    train_line,
    count(id) as no_of_incidents
  from 
    raw
  {{dbt_utils.group_by(4)}}
),

last_incident as (
    select 
        aest_created_date,
        reason,
        info_on_issue,
        train_line,
        no_of_incidents,
        lag(aest_created_date) over(tweets_window) as last_similar_incident,
        lead(aest_created_date) over(tweets_window) as next_similar_incident
    from 
        event_dates
    window 
        tweets_window as (partition by {{dbt_utils.generate_surrogate_key(['reason', 'info_on_issue','train_line'])}} order by aest_created_date asc)
),

final as (
    select
        aest_created_date,
        reason,
        info_on_issue,
        train_line,
        no_of_incidents,
        last_similar_incident,
        next_similar_incident,
        date_diff(aest_created_date,last_similar_incident, DAY) as days_since_last_incident
    from 
        last_incident
    where
        reason is not null
        and info_on_issue is not null
)

select 
    * 
from 
    final 
order by 
        train_line desc,
        reason,
        info_on_issue,
        aest_created_date desc
