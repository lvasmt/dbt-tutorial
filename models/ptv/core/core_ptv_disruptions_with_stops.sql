with ptv_disruptions_with_stops as (
    select 
        * 
    from {{ref('stg_ptv_disruptions_stops_union')}}
)

select 
    * 
from ptv_disruptions_with_stops