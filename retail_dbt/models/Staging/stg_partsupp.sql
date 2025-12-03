with source as (
    select * from {{ source('src', 'partsupp') }}
),

changed as (
    select
        ps_partkey as part_id,
        ps_suppkey as supplier_id,
        ps_availqty as available_quantity,
        ps_supplycost as cost,
        ps_comment as comment    
    from source
)
select * from changed