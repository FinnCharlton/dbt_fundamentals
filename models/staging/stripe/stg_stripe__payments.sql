select
    id as payment_id,
    orderid as order_id,
    created as created_date,
    paymentmethod as payment_method,
    amount / 100 as amount,
    status

from {{ source('stripe','payments') }}