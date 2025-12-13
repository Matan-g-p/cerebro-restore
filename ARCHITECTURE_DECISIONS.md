# Architecture & Design Decisions

This document outlines the rationale behind the architectural choices and project structure for the Cerebro Restore data platform.

## 1. Technology Stack Selection

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Orchestration** | **Apache Airflow** | Industry standard for DAG-based workflows. Python-native allows for flexible, complex dependency management and custom operators (e.g., interacting with MinIO). |
| **Transformation** | **dbt Core** | Brings software engineering best practices to SQL (testing, version control, documentation). Decouples transformation logic from orchestration code. |
| **Storage** | **MinIO** | S3-compatible object storage that runs locally. Allows simulating a cloud-native architecture in a self-contained Docker environment. |
| **Data Warehouse** | **PostgreSQL** | Reliable, ACID-compliant, and widely supported. Serves as both the transactional DB for Airflow and the analytical warehouse for this project scale. |
| **Infrastructure** | **Docker Compose** | Ensures reproducibility. "Infrastructure as Code" allows the entire platform to be spun up with a single command on any machine. |

## 2. Pipeline Architecture: ETLT (Extract-Transform-Load-Transform)

I chose an **ETLT** pattern over traditional ETL.

- **Extraction**: Airflow extracts the data from MinIO.
- **Transformation (Airflow)**: Since the biometrics data is very big, I've decided to transform it in Airflow (1m aggregations) instead of inserting the raw data.
- **Loading (Airflow)**: Airflow loads the data from MinIO and from the biometrics aggregations to the Data Warehouse (PostgreSQL).
- **Transformation (dbt)**: Once data is in the warehouse (`raw` schema), dbt takes over. This leverages the warehouse's compute power for transformations, which is more scalable and easier to debug than in-memory Python transformations.

## 3. Data Modeling Strategy

I implement a layered modeling approach (often called "Medallion Architecture"):

1. **Raw Layer (`raw` schema)**:
    - Exact copy of data from MinIO for heroes and missions (CSV/JSON).
    - Aggregation of the biometrics data in Airflow (1m aggregations).
    - *Goal*: Immutable history and auditability.

2. **Staging Layer (`staging` schema via dbt)**:
    - Light cleaning, type casting, and standardized naming.
    - *Goal*: Prepare data for joining and heavy logic.

3. **Marts Layer (`marts` schema via dbt)**:
    - Dimensional Modeling (Star Schema).
    - **Dimensions (`dim_`)**: `dim_heroes`, `dim_missions`. Contextual data.
    - **Facts (`fact_`)**: `fact_biometrics`. Measurable events.
    - *Goal*: Optimized for business logic and consistency.

4. **Analytics Layer (`analytics` schema)**:
    - **OBT (One Big Table)**: `obt_cerebro_restored`.
    - Denormalized wide table.
    - *Goal*: Performance and ease of use for end-user queries (like "The Hulk Factor") without complex joins at query time.
