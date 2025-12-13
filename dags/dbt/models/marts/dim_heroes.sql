
{{
  config(
    materialized = "incremental",
    unique_key = "hero_id",
    tags = ["marts", "heroes"]
  )
}}

select
    hero_id,
    name,
    alter_ego,
    first_appearance
from {{ ref('stg_heroes') }}
