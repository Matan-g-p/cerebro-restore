{{
  config(
    materialized = "view",
    tags = ["staging", "heroes"]
  )
}}


with source as (
    select * from {{ source('raw', 'heroes') }}
),

cleaned as (
    select
        hero_id,
        lower(REGEXP_REPLACE(name, '[^[:alnum:]]', '', 'g')) AS name,
        alter_ego,
        first_appearance
    from source
    where hero_id is not null
)
select 
	hero_id,
	max(name) as name,
	max(alter_ego) as alter_ego,
	max(first_appearance) as first_appearance
from cleaned
group by 1
