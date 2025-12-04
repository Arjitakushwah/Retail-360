{{ config(materialized='view') }}

with customers as (
    select * from {{ ref('stg_customer') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    c.customer_id,
    c.customer_name,
    count(o.order_id) as total_orders,
    sum(o.total_price) as customer_ltv
from customers c
left join orders o
    on c.customer_id = o.customer_id
group by
    c.customer_id,
    c.customer_name
