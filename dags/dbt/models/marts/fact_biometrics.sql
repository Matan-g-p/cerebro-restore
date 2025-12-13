
{{
  config(
    materialized = "incremental",
    unique_key = ["hero_id", "timestamp"],
    tags = ["marts", "biometrics"]
  )
}}

select
    timestamp,
    hero_id,
    heart_rate,
    stress_level,
    energy_output
from {{ ref('stg_biometrics') }}
where 1=1
{% if is_incremental() %}
    and timestamp > (select max(timestamp) from {{ this }})
{% endif %}