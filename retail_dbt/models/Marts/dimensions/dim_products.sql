{{ config(
    materialized = 'table',
    schema = 'MARTS'
) }}

with part as (
    select *
    from {{ ref('stg_part') }}
),

partsupp as (
    select 
        ps.part_id,
        ps.supplier_id,
        ps.available_quantity,
        ps.cost
    from {{ ref('stg_partsupp') }} ps
),

partsupp_agg as (
    select
        part_id,
        avg(cost) as supplier_cost_avg,
        min_by(supplier_id, cost) as primary_supplier
    from partsupp
    group by part_id
)

select
    {{ dbt_utils.generate_surrogate_key(['p.part_id']) }} as product_sk,
    p.part_id,
    p.name,
    p.brand,
    p.type,
    p.size,
    p.container,
    p.retail_price,
    a.primary_supplier,
    a.supplier_cost_avg

from part p
left join partsupp_agg a
    on p.part_id = a.part_id
