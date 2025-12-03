{{ config(materialized='table', schema='marts') }}

with hist as (
    select * from {{ ref('customer_scd2') }}
),

current_customer as (
    select *
    from hist
    where dbt_valid_to is null
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
<<<<<<< HEAD
    {{ dbt_utils.generate_surrogate_key(['c.customer_id'])}} as customer_sk,
=======
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
>>>>>>> 689a214826d7cd5b4e66b70bb1b98df6b6da3088
    c.customer_id,
    c.customer_name,
    c.address,
    n.name as nation,
    r.name as region,
    c.phone,
    c.market_segment,
    c.account_balance,
    c.dbt_valid_from,
    c.dbt_valid_to,
    true as current_flag
from current_customer c
left join nation n
    on c.nation_id = n.nation_id
left join region r
    on n.region_id = r.region_id

