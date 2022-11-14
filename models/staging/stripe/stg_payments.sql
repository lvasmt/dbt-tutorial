WITH raw_stripe AS (
    SELECT 
        id,
        orderid,
        paymentmethod,
        status,
        amount/100 as amount,
        created
    FROM 
        {{source('stripe','payment')}}
)

SELECT  
    *
FROM raw_stripe