# Cerebro Restore - Data Pipeline Project

A production-ready data engineering pipeline built with Apache Airflow, dbt, PostgreSQL, and MinIO. This project implements an ETLT (Extract, Transform, Load, Transform) architecture for processing and analyzing superhero mission data.

## 🏗️ Architecture

```mermaid
flowchart LR
    A["Data Generation"] --> B["MinIO Object Storage"]
    B --> C["Airflow DAG"]
    C --> D["PostgreSQL - Raw Layer"]
    D --> E["dbt Transformations"]
    E --> F["PostgreSQL - Analytics Layer"]
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#ffe1f5
    style D fill:#e1ffe1
    style E fill:#f5e1ff
    style F fill:#e1ffe1
```

**Components:**

- **Apache Airflow**: Orchestrates data pipelines and task scheduling
- **PostgreSQL** (2 instances):
  - Airflow metadata database (port 5432)
  - Data lake database (port 5433)
- **MinIO**: S3-compatible object storage for raw data files
- **dbt Core**: SQL-based data transformation framework

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 1.29 or higher)
- **Git**

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Matan-g-p/cerebro-restore.git
cd cerebro-restore
```

### 2. Configure Environment Variables

Copy the example environment file to the Docker directory and customize it with your credentials:

```bash
# On Windows (PowerShell)
Copy-Item .env.example .\Docker\.env

# On Linux/Mac
cp .env.example Docker/.env
```

Edit the `Docker/.env` file with your preferred credentials. **Important**: Change the default passwords for production use!

```env
# Example - Update these values
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_secure_password_here
DATALAKE_USER=admin
DATALAKE_PASSWORD=your_secure_password_here
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_secure_password_here
```

### 3. Start the Environment

Navigate to the Docker directory and start all services:

```bash
# On Windows (PowerShell)
cd Docker
docker-compose up -d

# On Linux/Mac
cd Docker
docker-compose up -d
```

If there is an issue with pulling images, try:

```bash
docker logout
```

**First-time setup**: The initial startup may take 2-3 minutes as Docker downloads images and initializes databases.

### 4. Verify Services are Running

Check that all containers are healthy:

```bash
docker-compose ps
```

You should see the following services running:

- `airflow-webserver`
- `airflow-scheduler`
- `postgres`
- `postgres-datalake`
- `minio`

### 5. Access the Web Interfaces

Once services are running, access the following URLs:

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| **Airflow UI** | <http://localhost:8080> | Username: `airflow`<br>Password: `airflow` (or your `.env` value) |
| **MinIO Console** | <http://localhost:9001> | Username: `minioadmin`<br>Password: `minioadmin` (or your `.env` value) |

## 📊 Running the Data Pipelines

The project includes two main DAGs that must be run in sequence:

### Step 1: Generate Mock Data

This DAG creates synthetic superhero data and uploads it to MinIO.

1. Open Airflow UI at <http://localhost:8080>
2. Log in with your credentials
3. Find the DAG named **`generate_and_ingest_data`**
4. Click the ▶️ (play) button to trigger the DAG
5. Wait for all tasks to complete (status: green)

**What it does:**

- Generates 10 unique heroes with duplicates and data quality issues
- Creates 50 mission logs with varying outcomes and missing data and timestamp issues
- Produces 50,000 biometric records with very high frequecy biometrics data
- Uploads all data to MinIO bucket `cerebro-data`

### Step 2: Run the ETL Pipeline

This DAG extracts data from MinIO, loads it into PostgreSQL, and runs dbt transformations.

1. In Airflow UI, find the DAG named **`cerebro_restore_pipeline`**
2. Click the ▶️ (play) button to trigger the DAG
3. Monitor the task execution

**What it does:**

- Creates database schemas (`raw` and `cerebro`)
- Ingests CSV, JSON, and Parquet files from MinIO
- Loads raw data into PostgreSQL staging tables
- Runs dbt transformations to create:
  - Staging models (cleaned and deduplicated data)
  - Dimension tables (`dim_heroes`, `dim_missions_heroes`)
  - Fact tables (`fact_biometrics`)
  - Analytics layer (One Big Table: `obt_cerebro_restored`)
- Truncates raw tables after successful transformation

### Verifying Results

Connect to the data lake database to query the results:

```bash
# Connect to PostgreSQL data lake
docker exec -it docker-postgres-datalake-1 psql -U admin -d cerebro

# Query the analytics layer
SELECT * FROM analytics.obt_cerebro_restored LIMIT 10;

# Check dimension tables
SELECT * FROM marts.dim_heroes;
SELECT * FROM marts.dim_missions_heroes;

# Exit
\q
```

## 📁 Project Structure

```
cerebro-restore/
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore rules
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── Docker/
│   └── docker-compose.yaml   # Docker services configuration
├── dags/
│   ├── cerebro_restore_pipeline.py    # Main ETL pipeline
│   ├── generate_and_ingest_data.py    # Data generation DAG
│   ├── helpers/
│   │   ├── minio/            # MinIO helper functions
│   │   └── postgres/         # PostgreSQL helper functions
│   └── dbt/
│       ├── dbt_project.yml   # dbt project configuration
│       ├── profiles.yml      # dbt connection profiles
│       └── models/
│           ├── staging/      # Staging models (data cleaning)
│           ├── marts/        # Dimension and fact tables
│           └── analytics/    # Analytics layer (OBT)
├── logs/                     # Airflow logs (gitignored)
└── plugins/                  # Airflow plugins
```

## 🧹 Cleanup

To stop and remove all containers, networks, and volumes:

```bash
# Stop services
docker-compose down

# Remove volumes (WARNING: This deletes all data!)
docker-compose down -v
```
