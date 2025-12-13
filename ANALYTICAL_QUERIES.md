# Analytical Queries

These queries are designed to be run against the `analytics.obt_cerebro_restored` table, which denormalizes heroes, missions, and biometrics data.

## 1. The Hulk Factor

**Question:** Which hero has the highest average `energy_output` during missions classified as "High Risk" (structural damage > 10,000)?

```sql
SELECT 
    hero_name,
    AVG(energy_output) as avg_energy_output
FROM analytics.obt_cerebro_restored
WHERE structural_damage > 10000 and mission_id is not null
GROUP BY hero_name
ORDER BY avg_energy_output DESC
LIMIT 1;
```

## 2. Reliability

**Question:** Which hero has participated in the most successful missions, and what is their average `stress_level` during those missions?

```sql
SELECT 
    hero_name,
    COUNT(DISTINCT mission_id) as successful_missions_count,
    AVG(stress_level) as avg_stress_level
FROM analytics.obt_cerebro_restored
WHERE mission_outcome = 'Successful'
GROUP BY hero_name
ORDER BY successful_missions_count DESC
LIMIT 1;
```
