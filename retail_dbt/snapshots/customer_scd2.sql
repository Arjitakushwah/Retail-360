{% snapshot customer_scd2 %}
{{
  config(
    target_schema='SNAPS',            -- where snapshot history will be stored
    unique_key='cust_key',            -- natural key from source
    strategy='check',                 -- create new record when check_cols change
    check_cols=['cust_name','cust_address','phone','acct_balance','market_segment']
  )
}}

select
  C_CUSTKEY   as cust_key,
  C_NAME      as cust_name,
  C_ADDRESS   as cust_address,
  C_NATIONKEY as nation_key,
  C_PHONE     as phone,
  C_ACCTBAL   as acct_balance,
  C_MKTSEGMENT as market_segment
from {{ source('raw','customer') }}

{% endsnapshot %}
