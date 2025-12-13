
{{
  config(
    materialized = "table",
    schema = "analytics",
    tags = ["analytics"]
  )
}}

with biometrics as (
    select * from {{ ref('stg_biometrics') }}
),

missions as (
    select * from {{ ref('stg_missions') }}
),

heroes as (
    select * from {{ ref('dim_heroes') }}
),

joined as (
    select
        b.timestamp,
        h.name as hero_name,
        h.alter_ego,
        h.hero_id,
        m.mission_id,
        m.location as mission_location,
        m.outcome as mission_outcome,
        cast(m.structural_damage as integer) as structural_damage,
        b.heart_rate,
        b.stress_level,
        b.energy_output
    from biometrics b
    left join missions m 
        on b.hero_id = m.hero_id 
        and b.timestamp = date_trunc('minute',m.timestamp)
    left join heroes h
        on b.hero_id = h.hero_id
)

select * from joined
