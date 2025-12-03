{% snapshot customer_scd2 %}
{{
  config(
    target_schema='SNAPS',          -- history will be stored in SNAPS schema
    unique_key='customer_id',      -- natural key from your staging model
    strategy='check',
    check_cols=[
      'customer_name',
      'address',
      'phone',
      'market_segment',
      'account_balance',
      'nation_key'
    ]
  )
}}

select
  customer_id,
  customer_name,
  address,
  nation_key,
  phone,
  account_balance,
  market_segment,
  current_timestamp() as snapshoted_at
from {{ ref('stg_customer') }}

{% endsnapshot %}
