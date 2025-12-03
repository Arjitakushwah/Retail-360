WITH source AS (
select
    L_ORDERKEY       as order_key,
    L_PARTKEY        as part_key,
    L_SUPPKEY        as supplier_key,
    L_LINENUMBER     as line_number,
    L_QUANTITY       as quantity,
    L_EXTENDEDPRICE  as extended_price,
    L_DISCOUNT       as discount,
    L_TAX            as tax,
    trim(L_RETURNFLAG)   as return_flag,
    trim(L_LINESTATUS)   as line_status,
    L_SHIPDATE       as ship_date,
    L_COMMITDATE     as commit_date,
    L_RECEIPTDATE    as receipt_date,
    trim(L_SHIPINSTRUCT) as ship_instruct,
    trim(L_SHIPMODE)     as ship_mode,
    trim(L_COMMENT)      as comment

from {{ source('raw', 'lineitem') }})

select * from source