WITH source AS (

    SELECT
        O_ORDERKEY::INTEGER        AS order_id,
        O_CUSTKEY::INTEGER         AS customer_id,
        TRIM(O_ORDERSTATUS)        AS order_status,
        O_TOTALPRICE::NUMBER(12,2) AS total_price,
        O_ORDERDATE::DATE          AS order_date,
        TRIM(O_ORDERPRIORITY)      AS order_priority,
        TRIM(O_CLERK)              AS clerk,
        O_SHIPPRIORITY::INTEGER    AS ship_priority,
        TRIM(O_COMMENT)            AS comment
    FROM {{ source('src', 'orders') }}

)

SELECT * FROM source