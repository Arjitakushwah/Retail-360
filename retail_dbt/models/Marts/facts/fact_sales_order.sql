{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',
    schema       = 'MARTS'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_dim as (
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
    where current_flag = true
),

date_dim as (
    select date_key, date_value
    from {{ source('marts','dim_dates') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id','o.customer_id']) }} as order_sk,
    o.order_id,
    cd.customer_sk,
    o.order_date as order_date,
    o.order_status,
    o.total_price,
    o.ship_priority
from orders o
left join customer_dim cd
    on o.customer_id = cd.customer_id
{% if is_incremental() %}
where o.order_date > (
    select max(order_date)
    from {{ this }} f
)
{% endif %}
