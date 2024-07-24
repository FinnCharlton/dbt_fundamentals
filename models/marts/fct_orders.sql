with payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

amount_per_order as (
    select 
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by 1
),

final as (
    select
        orders.order_id as order_id,
        orders.customer_id as customer_id,
        orders.order_date,
        coalesce (apo.amount, 0) as amount
    from
        orders join amount_per_order apo using (order_id)
)

select * from final