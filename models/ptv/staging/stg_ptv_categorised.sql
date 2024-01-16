WITH categorised AS (
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
        transport_type,
        CASE
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*buses replace trains.*|.*replacement buses.*|.*shuttle service.*|.*buses.*replac.*') THEN "Bus Replacements"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*clearing.*') THEN "Delays Clearing"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*major delays.*') THEN "Major Delays"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*delays.*') THEN "Delays"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*enter.*via.*') THEN "Different Station Entry"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*altered timetable.*') THEN "Altered Timetable"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*no.*lift.*|.*no.*escalator.*|.*lifts.*not in service.*') THEN "No lift/escalator access"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*direct to.*flinder.*|.*not?.*city loop.*') THEN "No City Loop / Direct to Flinders"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*trains will not stop .*station.*') THEN "Trains not stopping at specific stations"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*reduced service frequency.*') THEN "Reduced service frequency"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*will not run.*|.*cancelled.*|.*not running.*') THEN "Cancellations"
            ELSE disruption_type 
        END AS info_on_issue,
        CASE
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*level.?crossing.*work.*|.*project.*') then "Level Crossing Works"
            WHEN (REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*works.*|.*maintenance.*') and transport_type = 'metro_train') OR REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*track.*works.*') then "Maintenance & Planned Works"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*works.*|.*road.*work.*')  then "Road Works"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*outage.*') then "Power Outage"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*trespasser.*') then "Trespasser"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*fault.*|.*overhead.*|.*signalling systems.*|.*train.*removed from service.*|.*boom arm.*|.*boom gate.*') then "Equipment Fault"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*police.*|.*vicpol.*') then "Police Request"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*ill passenger.*|.*medical.*assistance.*') then "Sick Passenger"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*animal.*|.*dog.*|.*kangaroo.*|.*wildlife.*') then "Animals On Train Tracks"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*person.*hit.*|.*hit.*person|.*str..?k.*person.*|.*person.*struck.*by.*train.*') then "Person Hit By Train"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*operation.*incident.*|.*operation.*issue.*|.*drivers.*trains.*out of position.*') then "Operational Incident"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*operation.*request.*|.*operational requirement.*') then "Operational Request"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*service disruption|earlier disruption.*|.*previous .* service.*|.*earlier.*service.*|.*late arrival.*|.*delayed.*service.*|.*late.*train.*') then "Earlier Service Disruption/Delay"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*connect.*service.*|.*waiting for.*passenger.*') then "Connecting Service Disruption"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*vandal.*') then "Vandalism"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*disruptive passenger.*|.*unruly.*') then "Disruptive Passenger"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*flood.*') then "Flooding"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*hit.*bridge.*|.*truck.*hit.*bridge.*|.*str..?k.*bridge.*|.*bridge.*str..?k|.*bridge.*hit.*vehicle.*') then "Bridge Hit By Vehicle"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*weather.*|.*extreme heat.*|.*heavy rain.*|.*lightning strike.*|.*storm.*|.* wind.*') then "Weather related issues"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* fallen.*power.*lines.*|.*fallen.*post.*|.* obstruction.*track.*|.* debris.*|.* tree.*|.* vegetation .* track.*') then "Obstruction on path"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*vehicle.*block.*|.*car.*block.*|.*car.*stop.*|.*vehicle.*across.*|.*vehicle.*on.*|.*near hit.*') then "Vehicle Across Tracks"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*vehicle.*hit.*by.*train.*|vehicle.*struck.*by.*train.*|.*[car|bus|vehicle|truck|motor].*hit.*by.*train.*') then "Vehicle Hit By Train"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*frv.*|.*emergency.*|.*fire.*|.*ambulance.*|.*police.*|.*vicpol.*|.*cfa request.*|.*emergy services.*') then "Emergency Services Request"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*congestion.*') then "Congestion"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*game.*|.*concert.*|.*#AFL.*|.*match.*|.*day out.*|.*flemington racecourse.*|.*cricket.*|.*championship.*|.* event.*|.*Australian Open.*|.*Wodonga Gold Cup.*|.*New Year.*') then "Event in the city"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*accident.*|.*collision.*|.*colliding.*') then "Accident/Collision"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* traffic.*') then "Traffic"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* road closure.*') then "Road Closure"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* road block.*|.*not being able to turn right.*|.*block.*road.*') then "Road Blocked"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* incident.*') then "Incident"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*break.?down.*|.*broken.down.*|.*brake.*down.*') then "Vehicle Breakdown"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*goods train.*') then "Goods Train"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*full bus.*') then "Full Bus"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*protest.*|.*rally.*|.* realy. *') then "Protest/Rally"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*industrial action.*') then "Industrial Action"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*staff issue.*|.*delayed train crew.*|.*driver error.*') then "Staff Issues"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*driver error.*') then "Driver Error"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*PSO request.*') then "PSO request"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).* burst water.*|.*water pipeline burst.*') then "Burst Water Main"
            WHEN REGEXP_CONTAINS(CONCAT(title,description),r'(?i).*daylight savings.*') then "Daylight Savings"
            ELSE TRIM(REGEXP_EXTRACT(CONCAT(title,description),r'[dD]ue to [^\n\,\.]*')) 
        END AS reason
    FROM
        {{ref('stg_ptv_consolidated')}}
)

SELECT 
    *
FROM
    categorised