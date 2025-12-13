{{
  config(
    materialized = "incremental",
    unique_key = ["mission_id", "hero_id"],
    tags = ["marts", "missions"]
  )
}}

select
    mission_id,
    hero_id,
    location,
    timestamp,
    outcome,
    cast(structural_damage as integer) as structural_damage,
    cast(civilian_injuries as integer) as civilian_injuries
from {{ ref('stg_missions') }}
