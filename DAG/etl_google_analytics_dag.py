"""
ETL Google Analytics Airflow DAG
  
This DAG orchestrates the extraction, transformation, and loading (ETL) of Google Analytics data
from API endpoints into BigQuery tables. It runs every Wednesday at 6:00 AM and 6:00 PM, includes
data quality validation, and audit logging for data governance.
  
DAG Schedule:
- Runs at 6:00 AM and 6:00 PM on Wednesdays
- Retries: 2 attempts per task, 5-minute delay between retries
- Timeout: 3 minutes per task
  
Tasks:
1. Extract data from API endpoints and stage in GCS
2. Transform and clean the data
3. Validate data quality
4. Load to BigQuery staging tables
5. Upsert to final BigQuery tables
6. Log audit information
  
Monitoring & Governance:
- Data quality checks on required columns, nulls, duplicates, and record counts
- Audit logs captured in BigQuery
- Task-level timeout and retry policies
"""  
  
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
  
# Import your pipeline code (refactor your functions into a module for import)
import config_file as cf
from data_pipeline import (
    fetch_paginated_data,
    flatten_and_clean,
    run_data_quality_checks,
    load_to_staging,
    upsert_to_final,
    log_audit
)
  
# DAG configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5), 
    'execution_timeout': timedelta(minutes=3),
}
  
dag = DAG(
    dag_id='etl_google_analytics',
    description='ETL pipeline for Google Analytics data into BigQuery with data quality and audit logging.',
    schedule_interval='0 6,18 * * 3',  # 6:00 and 18:00 on Wednesdays
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['etl', 'google-analytics', 'bigquery', 'data-quality'],
    max_active_runs=1,
)
  
# --- Task Functions ---
  
def extract_api_data(endpoint_name, endpoint, **context):
    """
    Extract paginated data from API and upload raw JSON files to GCS.
    Returns records and source_files list.
    """
    from google.cloud import storage
    storage_client = storage.Client(project=cf.PROJECT_ID)
    bucket = storage_client.bucket(cf.BUCKET_NAME)
    records, source_files = fetch_paginated_data(endpoint, endpoint_name, bucket)
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key=f"{endpoint_name}_records", value=records)
    context['ti'].xcom_push(key=f"{endpoint_name}_source_files", value=source_files)
    return True
  
def transform_and_clean_task(endpoint_name, **context):
    """
    Flatten and sanitize extracted records, drop duplicates if needed.
    """
    records = context['ti'].xcom_pull(key=f"{endpoint_name}_records")
    df = flatten_and_clean(endpoint_name, records)
    context['ti'].xcom_push(key=f"{endpoint_name}_df", value=df)
    return True
  
def data_quality_check_task(endpoint_name, **context):
    """
    Perform data quality checks (nulls, duplicates, required columns, record count).
    If failed, log audit and short-circuit further ETL for this endpoint.
    """
    df = context['ti'].xcom_pull(key=f"{endpoint_name}_df")
    valid, dq_issues = run_data_quality_checks(df, endpoint_name)
    source_files = context['ti'].xcom_pull(key=f"{endpoint_name}_source_files")
    if not valid:
        log_audit(endpoint_name, len(df), f"FAILED: Data Quality – {dq_issues}", source_files)
        # Short-circuit further tasks for this branch
        return False
    return True
  
def load_staging_task(endpoint_name, **context):
    """
    Load cleaned dataframe into BigQuery staging table.
    """
    df = context['ti'].xcom_pull(key=f"{endpoint_name}_df")
    load_to_staging(df, endpoint_name)
    return True
  
def upsert_final_task(endpoint_name, **context):
    """
    Upsert staged data into final BigQuery table using MERGE.
    """
    upsert_to_final(endpoint_name)
    return True
  
def audit_logging_task(endpoint_name, **context):
    """
    Log successful load audit into BigQuery.
    """
    df = context['ti'].xcom_pull(key=f"{endpoint_name}_df")
    source_files = context['ti'].xcom_pull(key=f"{endpoint_name}_source_files")
    log_audit(endpoint_name, len(df), "SUCCESS", source_files)
    return True
  
# --- DAG Tasks for each endpoint ---
  
endpoints = {
    "daily_visits": "daily-visits",
    "ga_sessions": "ga-sessions-data"
}
  
for endpoint_name, endpoint in endpoints.items(): 
    extract = PythonOperator(
        task_id=f'extract_{endpoint_name}',
        python_callable=extract_api_data,
        op_kwargs={'endpoint_name': endpoint_name, 'endpoint': endpoint},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        retries=2
    )  
    transform = PythonOperator(
        task_id=f'transform_{endpoint_name}',
        python_callable=transform_and_clean_task,
        op_kwargs={'endpoint_name': endpoint_name},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        retries=2
    )
    dq_check = ShortCircuitOperator(
        task_id=f'dq_check_{endpoint_name}',
        python_callable=data_quality_check_task,
        op_kwargs={'endpoint_name': endpoint_name},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        retries=2
    )
    load_staging = PythonOperator(
        task_id=f'load_staging_{endpoint_name}',
        python_callable=load_staging_task,
        op_kwargs={'endpoint_name': endpoint_name},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        retries=2
    )
    upsert_final = PythonOperator(
        task_id=f'upsert_final_{endpoint_name}',
        python_callable=upsert_final_task,
        op_kwargs={'endpoint_name': endpoint_name},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        retries=2
    )
    audit_log = PythonOperator(
        task_id=f'audit_log_{endpoint_name}',
        python_callable=audit_logging_task,
        op_kwargs={'endpoint_name': endpoint_name},
        dag=dag,
        execution_timeout=timedelta(minutes=3),
        trigger_rule=TriggerRule.ALL_DONE, # log audit even on failure
        retries=2
    )
  
    # Set dependencies
    extract >> transform >> dq_check >> load_staging >> upsert_final >> audit_log
    # If dq_check fails, audit_log will still run (thanks to trigger_rule=ALL_DONE)
  
# End of DAG file