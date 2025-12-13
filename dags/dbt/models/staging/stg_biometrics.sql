{{
  config(
    materialized = "view",
    tags = ["staging", "biometrics"]
  )
}}

with source as (
    select * from {{ source('raw', 'biometrics') }}
)

select
    timestamp::timestamp as timestamp,
    hero_id,
    heart_rate,
    stress_level,
    energy_output
from source
where hero_id is not null
and timestamp is not null
