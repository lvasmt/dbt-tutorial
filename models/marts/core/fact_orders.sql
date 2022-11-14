WITH orders AS (
    SELECT  
        order_id,
        customer_id
    FROM
        {{ref('stg_orders')}}
),

payments AS (
    SELECT
        orderid,
        paymentmethod,
        status,
        amount
    FROM
        {{ref('stg_payments')}}
),

payments_summed AS (
    SELECT 
        orderid,
        SUM(amount) AS amount
    FROM 
        payments
    WHERE
        status = 'success'
    GROUP BY 
        orderid
),

combined AS (
    SELECT 
        order_id,
        customer_id,
        amount
    FROM 
        orders 
        LEFT JOIN payments_summed
            ON orders.order_id = payments_summed.orderid
)

SELECT 
    order_id,
    customer_id,
    amount
FROM 
    combined