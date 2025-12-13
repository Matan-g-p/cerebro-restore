"""
Centralized configuration settings for Cerebro Restore project.

All configuration values are environment-aware and can be overridden
via environment variables.
"""

import os


class Config:
    """Central configuration class for all project settings."""
    
    # =========================================================================
    # MinIO / Object Storage Configuration
    # =========================================================================
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "cerebro-data")
    
    # =========================================================================
    # Database Schema Configuration
    # =========================================================================
    RAW_SCHEMA = "raw"
    STAGING_SCHEMA = "staging"
    MARTS_SCHEMA = "marts"
    ANALYTICS_SCHEMA = "analytics"
    
    # =========================================================================
    # Data Generation Configuration
    # =========================================================================
    NUM_HEROES = int(os.getenv("NUM_HEROES", "10"))
    NUM_MISSIONS = int(os.getenv("NUM_MISSIONS", "50"))
    NUM_BIOMETRIC_RECORDS = int(os.getenv("NUM_BIOMETRIC_RECORDS", "50000"))
    
    # =========================================================================
    # Airflow Connection IDs
    # =========================================================================
    POSTGRES_CONN_ID = "cerebro_db"
    AWS_CONN_ID = "aws_default"
    
    # =========================================================================
    # Logging Configuration
    # =========================================================================
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
