# Entity Relationship Diagram

```mermaid
erDiagram
    dim_heroes ||--o{ dim_missions_heroes : "participates in"
    dim_heroes ||--o{ fact_biometrics : "has metrics"

    dim_heroes {
        string hero_id PK
        string name
        string alter_ego
        date first_appearance
    }

    dim_missions_heroes {
        string mission_id PK
        string hero_id FK
        string location
        timestamp timestamp
        string outcome
    }

    fact_biometrics {
        string hero_id FK
        timestamp timestamp PK
        int heart_rate
        float stress_level
        int energy_output
    }
```

## Table Descriptions

- **dim_heroes**: Contains static metadata about heroes (names, IDs).
- **dim_missions_heroes**: Links missions to heroes. Since missions can have multiple heroes, this acts as a bridge table (or factless fact table) with mission details.
- **fact_biometrics**: Time-series data containing biometric readings for each hero.
