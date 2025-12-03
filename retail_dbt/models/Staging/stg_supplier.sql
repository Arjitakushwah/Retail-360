
with source as (
    select * from {{ source('src', 'supplier') }}
),

supp as (
    select
        s_suppkey      as supplier_id,
        s_name         as name,
        s_address      as address,
        s_nationkey    as nation_id,
        s_phone        as phone,
        s_acctbal      as account_balance,
        s_comment      as comment
    from source
)

select * from supp
