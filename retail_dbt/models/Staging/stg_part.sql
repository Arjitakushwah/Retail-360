with source as (
    select * 
    from {{ source('src', 'part') }}
),

parts as (
    select
        p_partkey      as part_id,
        p_name         as name,
        p_type         as type,
        p_size         as size,
        p_mfgr         as manufacturer,
        p_brand        as brand,
        p_comment      as comment,
        p_container    as container,
        p_retailprice  as retail_price

    from source
)

select * 
from parts
