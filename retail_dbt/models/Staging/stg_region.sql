with regions as (
select 
   r_regionkey as region_id,
   r_name as name,
   r_comment as comment
from {{source('src','region')}}
)

select * from regions
