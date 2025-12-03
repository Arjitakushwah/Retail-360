{{ config (
    materialized = 'table', schema = 'marts'
)}}

with supplier as (
    select *
    from {{ ref('stg_supplier') }}
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
    s.supplier_id,
    s.name,
    s.account_balance,
    s.phone,
    s.address,
    n.name as nation,
    r.name as region,
    
from supplier s
join nation n
    on s.nation_id = n.nation_id
join region r
    on n.region_id = r.region_id
     