{{ config(materialized='table',schema='marts') }}

with customer as (
    select *
    from {{ ref('stg_customer') }}
),

nation as (
    select *
    from {{ ref('stg_nation') }}
),

region as (
    select *
    from {{ ref('stg_region') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['c_custkey'])}} as customer_sk,
    c.customer_id,
    c.customer_name,
    c.address,
    n.name as nation,
    r.name as region,
    c.phone,
    c.market_segment,
    c.account_balance
from customer c
left join nation n
    on c.nation_key = n.nation_id
left join region r
    on n.region_id = r.region_id
