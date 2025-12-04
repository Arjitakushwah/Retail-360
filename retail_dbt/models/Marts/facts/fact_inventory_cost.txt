{{ config(
    materialized = 'incremental',
    unique_key = 'inventory_cost_sk',
    schema = 'MARTS'
) }}

with ps as (
    select * from {{ ref('stg_partsupp') }}
),

product_dim as (
    select part_id, product_sk
    from {{ ref('dim_products') }}
),

supplier_dim as (
    select supplier_id,supplier_sk
    from {{ ref('dim_supplier') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ps.part_id','ps.supplier_id']) }} as inventory_cost_sk,
    ps.part_id,
    ps.supplier_id,
    pd.product_sk,
    sd.supplier_sk,
    ps.cost,
    ps.available_quantity
from ps
left join product_dim pd
    on ps.part_id = pd.part_id
left join supplier_dim sd
    on ps.supplier_id = sd.supplier_id

{% if is_incremental() %}
where ps.cost > (
    select coalesce(max(cost),0)
    from {{ this }}
)
{% endif %}