{{ config(
    materialized = 'incremental',
    unique_key   = 'sales_line_sk',
    schema       = 'MARTS'
) }}

with li as (
    select *
    from {{ ref('stg_lineitem') }}
),

orders as (
    select *
    from {{ ref('stg_orders') }}
),

customer_dim as (
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
    where current_flag = true
),

date_dim as (
    select 
        date_key,
        date_value
    from {{ source('marts','dim_dates') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['li.order_id','li.line_number']) }} as sales_line_sk,
    li.order_id,
    o.customer_id,
    cd.customer_sk,
    li.part_id,
    li.supplier_id,
    o.order_date as order_date,
    li.ship_date  as ship_date,
    li.receipt_date  as receipt_date,
    o.total_price,
    o.ship_priority
from li
left join orders o
    on li.order_id = o.order_id
left join customer_dim cd
    on o.customer_id = cd.customer_id

{% if is_incremental() %}
where o.order_date > (
    select max(order_date)
    from {{ this }} 
)
{% endif %}
