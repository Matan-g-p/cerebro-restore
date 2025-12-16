# Analytical Queries

These queries are designed to be run against the `analytics.cerebro_agg_table` table, which denormalizes heroes, missions, and biometrics data.

## 1. The Hulk Factor

**Question:** Which hero has the highest average `energy_output` during missions classified as "High Risk" (structural damage > 10,000)?

```sql
SELECT 
    hero_name,
    AVG(energy_output) as avg_energy_output
FROM analytics.cerebro_agg_table
WHERE structural_damage > 10000
GROUP BY hero_name
ORDER BY avg_energy_output DESC NULLS LAST
LIMIT 1;
```

## 2. Reliability

**Question:** Which hero has participated in the most successful missions, and what is their average `stress_level` during those missions?

```sql
SELECT 
    hero_name,
    COUNT(DISTINCT mission_id) as successful_missions_count,
    AVG(stress_level) as avg_stress_level
FROM analytics.cerebro_agg_table
WHERE mission_outcome = 'Successful'
GROUP BY hero_name
ORDER BY successful_missions_count DESC NULLS LAST
LIMIT 1;
```
