import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta, timezone

# --- Configuration ---
DATA_DIR = 'data'
NUM_HEROES = 10
NUM_MISSIONS = 50
NUM_BIOMETRIC_RECORDS = 50000

# Ensure the data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# --- 1. HERO REGISTRY (heroes.csv) ---
def generate_heroes_csv(filename):
    """Generates the heroes.csv file with duplicates and messy formatting."""
    print("Generating heroes.csv...")
    
    # 10 Unique Heroes
    unique_heroes = [
        ("C-1", "Captain America", "Steve Rogers", "1941-03-01"),
        ("I-2", "Iron Man", "Tony Stark", "1963-03-01"),
        ("T-3", "Thor", "Donald Blake", "1962-08-01"),
        ("H-4", "The Hulk", "Bruce Banner", "1962-05-01"),
        ("S-5", "Spider-Man", "Peter Parker", "1962-08-01"),
        ("W-6", "Black Widow", "Natasha Romanoff", "1964-04-01"),
        ("D-7", "Doctor Strange", "Stephen Strange", "1963-07-01"),
        ("V-8", "Vision", "Vision", "1968-10-01"),
        ("P-9", "Black Panther", "T'Challa", "1966-07-01"),
        ("A-10", "Ant-Man", "Scott Lang", "1979-03-01")
    ]
    
    # Create the base DataFrame
    data = []
    for hero_id, name, alter_ego, appearance in unique_heroes:
        data.append({'hero_id': hero_id, 'name': name, 'alter_ego': alter_ego, 'first_appearance': appearance})

    df = pd.DataFrame(data)

    # --- CATCH 1: Contains duplicates ---
    # Duplicate Iron Man and Spider-Man
    df = pd.concat([df, df[df['hero_id'] == 'I-2'], df[df['hero_id'] == 'S-5'].copy()], ignore_index=True)

    # --- CATCH 2: Messy formatting ---
    # Messy formatting for Spider-Man: S-5
    df.loc[df['hero_id'] == 'S-5', 'name'] = df.loc[df['hero_id'] == 'S-5', 'name'].apply(
        lambda x: "spiderman" if np.random.rand() < 0.5 else "SPIDER-MAN" if np.random.rand() < 0.5 else x
    )
    # Messy formatting for The Hulk: H-4
    df.loc[df['hero_id'] == 'H-4', 'name'] = df.loc[df['hero_id'] == 'H-4', 'name'].apply(
        lambda x: "the hulk" if np.random.rand() < 0.5 else "HULK"
    )
    # Introduce nulls in 'alter_ego' (Data Quality check target)
    df.loc[df.index[-1], 'hero_id'] = None
    df.loc[df.index[-2], 'name'] = None
    
    df.to_csv(os.path.join(DATA_DIR, filename), index=False)
    print(f"-> {filename} generated with {len(df)} records (10 unique heroes + duplicates/nulls).")
    return df['hero_id'].dropna().unique().tolist()

# --- 2. MISSION LOGS (mission_logs.json) ---
def generate_mission_logs_json(filename, hero_ids):
    """Generates the mission_logs.json file with nested data, missing fields, and timezone issues."""
    print("Generating mission_logs.json...")
    
    logs = []
    start_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    
    for i in range(1, NUM_MISSIONS + 1):
        mission_id = f"M-{i:03d}"
        
        # Determine participating heroes (1 to 4)
        num_heroes = np.random.randint(1, 5)
        participating_hero_ids = np.random.choice(hero_ids, num_heroes, replace=False).tolist()

        # Randomly choose a timezone for the timestamp
        tz = timezone.utc if np.random.rand() < 0.7 else timezone(timedelta(hours=-5)) # UTC or EST
        
        log = {
            "mission_id": mission_id,
            "hero_ids": participating_hero_ids,
            "location": np.random.choice(["New York", "Wakanda", "Sokovia", "London", "Sanctuary"]),
            
            # --- CATCH 1: Timestamps are in different timezones ---
            "timestamp": (start_time + timedelta(hours=i)).astimezone(tz).isoformat(),
            
            "outcome": np.random.choice(["Successful", "Failure", "Draw"]),
            
            # --- CATCH 2: Nested damage_report dict ---
            "damage_report": {
                "structural_damage": int(np.random.normal(loc=8000, scale=5000)), # For 'High Risk' check (>10k)
                "civilian_injuries": np.random.randint(0, 20)
            }
        }
        
        # --- CATCH 3: Some fields are missing (randomly drop damage_report) ---
        if np.random.rand() < 0.1:
            log.pop("damage_report")
        
        # Randomly make 'location' missing
        if np.random.rand() < 0.05:
            log["location"] = None
            
        logs.append(log)
        
    with open(os.path.join(DATA_DIR, filename), 'w') as f:
        json.dump(logs, f, indent=4)
        
    print(f"-> {filename} generated with {NUM_MISSIONS} logs.")
    return logs

# --- 3. BIOMETRIC TELEMETRY (biometrics.parquet) ---
def generate_biometrics_parquet(filename, hero_ids):
    """Generates the biometrics.parquet file simulating high-frequency sensor data (Big Data)
    that needs to be aggregated/downsampled."""
    print("Generating biometrics.parquet...")
    
    # 1-second interval data over a period (e.g., about 14 hours total)
    timestamps = pd.to_datetime(pd.date_range(
        start=datetime(2025, 1, 1, 0, 0, 0),
        periods=NUM_BIOMETRIC_RECORDS,
        freq='1s',
        tz=timezone.utc
    ))
    
    # Randomly assign hero_ids to the records
    df = pd.DataFrame({
        'timestamp': timestamps,
        'hero_id': np.random.choice(hero_ids, NUM_BIOMETRIC_RECORDS, replace=True),
        'heart_rate': np.random.normal(loc=85, scale=15, size=NUM_BIOMETRIC_RECORDS).clip(50, 150).astype(int),
        
        # Stress level tied to high energy output for a scenario
        'stress_level': np.random.uniform(0, 10, size=NUM_BIOMETRIC_RECORDS), 
        'energy_output': np.random.exponential(scale=500, size=NUM_BIOMETRIC_RECORDS).clip(0, 5000).astype(int)
    })
    
    # Introduce some nulls for robustness
    df.loc[df.sample(frac=0.01).index, 'heart_rate'] = None
    
    # --- CATCH: High-frequency data that needs efficient aggregation (1-minute downsampling) ---
    df.to_parquet(os.path.join(DATA_DIR, filename), index=False)
    
    print(f"-> {filename} generated with {NUM_BIOMETRIC_RECORDS} high-frequency records.")


# --- Main Execution ---
if __name__ == "__main__":
    
    # 1. Generate heroes.csv
    valid_hero_ids = generate_heroes_csv("heroes.csv")
    
    # 2. Generate mission_logs.json (requires hero IDs from step 1)
    generate_mission_logs_json("mission_logs.json", valid_hero_ids)
    
    # 3. Generate biometrics.parquet (requires hero IDs from step 1)
    generate_biometrics_parquet("biometrics.parquet", valid_hero_ids)
    
    print("\n✅ Data Generation Complete. Files are in the 'data' directory.")