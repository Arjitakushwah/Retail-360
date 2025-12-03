with nations as (
select 
   n_nationkey as nation_id,
   n_name as name,
   n_regionkey as region_id,
   n_comment as comment
from {{ source('src','nation')}}
)
select * from nations