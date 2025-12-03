WITH source AS (
    SELECT 
    C_CUSTKEY      as customer_id,
    trim(C_NAME)    as customer_name,
    trim(C_ADDRESS) as address,
    C_NATIONKEY    as nation_id,
    trim(C_PHONE)  as phone,
    C_ACCTBAL      as account_balance,
    trim(C_MKTSEGMENT) as market_segment,
    trim(C_COMMENT)    as comment
    FROM {{ source('src', 'customer') }}
)

SELECT * FROM source