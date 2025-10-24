"""
DISH Data Pipeline Package
This allows importing functions for Airflow DAG and CLI runs.
"""
from .data_pipeline import (
    fetch_paginated_data,
    flatten_and_clean,
    run_data_quality_checks,
    load_to_staging,
    upsert_to_final,
    log_audit
)
