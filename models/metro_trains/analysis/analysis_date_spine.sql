{{
  config(
    materialized = "table",
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["train_line","info_on_issue"]
  )
}}

with date_spine as (
  select
    *
  FROM 
    UNNEST(GENERATE_DATE_ARRAY((select min(aest_created_date) from {{ref('core_metro_trains')}}), (select max(aest_created_date) from {{ref('core_metro_trains')}}), INTERVAL 1 DAY)) AS date_spine
 ),

dsli as (
    select 
        aest_created_date,
        reason,
        info_on_issue,
        train_line,
        no_of_incidents,
        last_similar_incident,
        next_similar_incident,
        days_since_last_incident
    from 
        {{ref('analysis_days_since_last_incident')}}
),

train_spine as (
    select distinct 
        train_line 
    from 
        dsli 
    where 
        train_line is not null
), 

reason_spine as (
    select distinct 
        reason
    from 
        dsli 
    where 
        reason is not null
), 

issue_spine as (
    select distinct 
        info_on_issue
    from 
        dsli 
    where 
        info_on_issue is not null
), 

full_spine as (
    select 
        date_spine.date_spine as date, 
        train_line,
        reason,
        info_on_issue
    from 
        date_spine
        cross join train_spine
        cross join reason_spine
        cross join issue_spine
    order by 
        train_line,
        reason, 
        info_on_issue,
        date_spine.date_spine
),

joined as (
    select 
        fs.date,
        fs.train_line,
        fs.reason,
        fs.info_on_issue,
        t1.aest_created_date as latest_incident_date,
        --ifnull(t1.last_similar_incident,t1.aest_created_date) as last_similar_incident, 
        t1.last_similar_incident as previous_incident_date,
        t1.next_similar_incident as next_incident_date,
        date_diff(fs.date,t1.aest_created_date, DAY) as days_since_last_incident
    from 
        full_spine as fs 
        full join dsli as t1 on 
            fs.reason = t1.reason 
            and fs.train_line = t1.train_line 
            and fs.info_on_issue = t1.info_on_issue
            and (
                    (fs.date >=t1.aest_created_date and fs.date < t1.next_similar_incident)
                    or (fs.date >=t1.aest_created_date and t1.next_similar_incident is null)
                )
        
)

select 
    *
from 
    joined
where 
    latest_incident_date is not null