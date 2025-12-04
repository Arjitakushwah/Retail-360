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
    od.date_key  as order_date_sk,
    sd.date_key  as ship_date_sk,
    rd.date_key  as receipt_date_sk,
    o.total_price,
    o.ship_priority
from li
left join orders o
    on li.order_id = o.order_id
left join customer_dim cd
    on o.customer_id = cd.customer_id
left join date_dim od
    on o.order_date = od.date_value
left join date_dim sd
    on li.ship_date = sd.date_value
left join date_dim rd
    on li.receipt_date = rd.date_value

{% if is_incremental() %}
where li.ship_date > (
    select max(d.date_value)
    from {{ this }} f
    join {{ source('marts','dim_dates') }} d
      on f.ship_date_sk = d.date_key
)
{% endif %}
