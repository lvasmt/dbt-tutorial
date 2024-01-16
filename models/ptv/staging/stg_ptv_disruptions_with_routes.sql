WITH routes AS (
  SELECT 
  	* 
  FROM 
  	{{ref('stg_ptv_routes_consolidated')}}
),

disruptions AS (
  SELECT 
  	* 
  FROM 
  	{{ref('stg_ptv_categorised')}}
 ),
joined AS (
  SELECT 
  	routes.disruption_id as disruption_id,
    routes.route_name,
    routes.route_type,
  	routes.route_number,
    disruptions.file_date,
    disruptions.from_date,
    disruptions.last_updated,
    disruptions.published_on,
    to_date,
    disruptions.title,
    disruptions.description,
    disruptions.disruption_status,
    disruptions.disruption_type,
    INITCAP(REGEXP_REPLACE(disruptions.transport_type,"_"," ")) AS transport_type,
  	disruptions.info_on_issue,
    disruptions.reason
  FROM 
  	routes
  LEFT JOIN 
    disruptions
  ON 
    disruptions.disruption_id = routes.disruption_id
    AND disruptions.file_date = routes.file_date
)

select * from joined