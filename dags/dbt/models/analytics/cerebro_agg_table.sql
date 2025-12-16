
{{
  config(
    materialized = "incremental",
    unique_key = "date",
    schema = "analytics",
    tags = ["analytics"]
  )
}}

with biometrics as (
    select 
        * 
    from {{ ref('fact_biometrics') }}
    where 1=1
    {% if is_incremental() %}
        and date(timestamp) >= (select max(date(timestamp)) from {{ this }})
    {% endif %}
),
missions as (
    select 
        * 
    from {{ ref('dim_missions_heroes') }}
    where 1=1
    {% if is_incremental() %}
        and date(timestamp) >= (select max(date(timestamp)) from {{ this }})
    {% endif %}
),
heroes as (
    select 
        * 
    from {{ ref('dim_heroes') }}
)
select
    m.timestamp as mission_timestamp,
    date(m.timestamp) as mission_date,
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
from missions m 
left join biometrics b
     on m.hero_id = b.hero_id 
    and date_trunc('minute', m.timestamp) = b.timestamp
left join heroes h
     on m.hero_id = h.hero_id