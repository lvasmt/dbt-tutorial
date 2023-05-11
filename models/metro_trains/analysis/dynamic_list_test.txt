{% set reasons = dbt_utils.get_column_values(table=ref('analysis_date_spine'), column='reason') %}
{%- set reasons_list %} 
    {%- for reason in reasons %}
        '{{reason}}'
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
{% endset %}

{% set issues = dbt_utils.get_column_values(table=ref('analysis_date_spine'), column='info_on_issue') %}
{%- set issues_list %} 
    {%- for issue in issues %}
        '{{issue}}'
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
{% endset %}



with pivoted as (
  select * from 
  (
    select 
        date_spine,
        --last_similar_incident,
        reason,
        info_on_issue,
        train_line,
        days_since_last_incident
    from
      {{ref('analysis_date_spine')}} )  
  pivot (sum(days_since_last_incident) for reason in ({{reasons_list}}))
  order by date_spine desc
)

select 
    * 
from 
    pivoted
order by 
    train_line asc, 
    info_on_issue asc, 
    date_spine asc

    