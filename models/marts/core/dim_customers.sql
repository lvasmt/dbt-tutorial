{{ config (
    materialized="table"
)}}
with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from {{source('jaffle_shop','customers')}}

),

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from {{source('jaffle_shop','orders')}}

),

payments AS (
    SELECT 
        order_id,
        SUM(amount) AS amount
    FROM 
        {{ref('fact_orders')}}
    GROUP BY 
        order_id
),

customer_orders as (

    select
        customer_id,

        min(order_date)         as first_order_date,
        max(order_date)         as most_recent_order_date,
        count(orders.order_id)  as number_of_orders,
        sum(amount)             as lifetime_value

    from orders
    left join payments
        on orders.order_id = payments.order_id

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders, 
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)

)

select * from final