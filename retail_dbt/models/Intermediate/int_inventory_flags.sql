{{ config(materialized='view') }}

with parts as (
    select * from {{ ref('stg_part') }}
),

partsupp as (
    select * from {{ ref('stg_partsupp') }}
),

sales as (
    select
        part_id,
        sum(quantity) as total_sold
    from {{ ref('stg_lineitem') }}
    group by part_id
)

select
    p.part_id,
    p.name,
    ps.available_quantity,
    s.total_sold,

    case 
        when ps.available_quantity < 50 then 'LOW_STOCK'
        else 'OK'
    end as stock_flag,

    case
        when s.total_sold > 500 then 'HIGH_DEMAND'
        else 'NORMAL'
    end as demand_flag

from parts p
left join partsupp ps
    on p.part_id = ps.part_id
left join sales s
    on p.part_id = s.part_id
