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
    dd.date_key as order_date_sk,
    o.order_status,
    o.total_price,
    o.ship_priority
from orders o
left join customer_dim cd
    on o.customer_id = cd.customer_id

left join date_dim dd
    on o.order_date = dd.date_value
{% if is_incremental() %}
where o.order_date > (
    select max(d.date_value)
    from {{ this }} f
    join {{ source('marts','dim_dates') }} d
      on f.order_date_sk = d.date_key
)
{% endif %}
