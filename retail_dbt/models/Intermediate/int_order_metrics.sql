{{ config(materialized='view') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

lineitems as (
    select * from {{ ref('stg_lineitem') }}
)

select
    o.order_id,
    o.customer_id,

    count(li.line_number) as total_items,

    sum(
        case when li.return_flag = 'R' then 1 else 0 end
    ) as returned_items,

    sum(
        case when o.order_status = 'C' then 1 else 0 end
    ) as cancelled_flag

from orders o
left join lineitems li
    on o.order_id = li.order_id

group by
    o.order_id,
    o.customer_id,
    o.order_status
