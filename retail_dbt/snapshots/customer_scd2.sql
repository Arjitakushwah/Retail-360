{% snapshot customer_scd2 %}
{{
  config(
    target_schema='SNAPS',          
    unique_key='customer_id',      
    strategy='check',
    check_cols=[
      'customer_name',
      'address',
      'phone',
      'market_segment',
      'account_balance',
      'nation_id'
    ]
  )
}}

select
  customer_id,
  customer_name,
  address,
  nation_id,
  phone,
  account_balance,
  market_segment,
  current_timestamp() as snapshoted_at
from {{ ref('stg_customer') }}

{% endsnapshot %}
