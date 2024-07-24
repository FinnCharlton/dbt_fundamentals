with payments as (
    select * from {{ ref('stg_stripe__payments')}}
),

pivoted as (
    select
        order_id,
        {% set methods = ['bank_transfer','coupon','credit_card','gift_card'] %}
        {% for method in methods %}
        sum(case when payment_method = '{{method}}' then amount else 0 end) as {{method}}_amount,
        {% endfor %}
    from
        payments
    group by
        1
)

select * from pivoted