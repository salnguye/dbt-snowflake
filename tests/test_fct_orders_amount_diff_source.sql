with staging as (
    select order_id,
    sum(amount) as staging_amount

    from {{ ref('stg_payments') }}
    where status = 'success'
    group by order_id
), target as (
    select order_id,
    sum(amount) as target_amount

    from {{ ref ('fct_orders') }}
    group by order_id
)
select a.order_id,
a.staging_amount,
b.target_amount

from staging a inner join target b using (order_id)
where a.staging_amount <> b.target_amount