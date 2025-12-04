{{ config (
    materialized = 'table', schema = 'MARTS'
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
    {{ dbt_utils.generate_surrogate_key(['s.supplier_id']) }} as supplier_sk,
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
     