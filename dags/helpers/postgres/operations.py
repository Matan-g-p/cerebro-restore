from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_tables():
    """Creates the target tables in Postgres if they don't exist."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Heroes Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS heroes (
            hero_id VARCHAR(50),
            name VARCHAR(100),
            alter_ego VARCHAR(100),
            first_appearance VARCHAR(50),
            is_valid BOOLEAN DEFAULT TRUE
        );
    """)
    
    # Missions Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS missions (
            mission_id VARCHAR(50),
            hero_ids TEXT, 
            location VARCHAR(100),
            timestamp TIMESTAMP,
            outcome VARCHAR(50),
            structural_damage INTEGER,
            civilian_injuries INTEGER
        );
    """)
    
    # Biometrics Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS biometrics (
            timestamp TIMESTAMP,
            hero_id VARCHAR(50),
            heart_rate INTEGER,
            stress_level FLOAT,
            energy_output INTEGER
        );
    """)
    print("Tables created successfully.")