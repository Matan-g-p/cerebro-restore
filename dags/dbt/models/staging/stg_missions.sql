{{
  config(
    materialized = "view",
    tags = ["staging", "missions"]
  )
}}


with source as (
    select * from {{ source('raw', 'missions') }}
),

flattened as (
    select
        mission_id,
        jsonb_array_elements_text(hero_ids::jsonb) as hero_id,
        location,
        timestamp::timestamptz at time zone 'UTC' as timestamp,
        outcome,
        (damage_report::jsonb)->>'structural_damage' as structural_damage,
        (damage_report::jsonb)->>'civilian_injuries' as civilian_injuries
    from source
    where mission_id is not null
)

select distinct * from flattened
