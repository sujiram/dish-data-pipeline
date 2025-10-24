## DAG Documentation & Task Descriptions
# DAG Overview
Name: etl_google_analytics
Purpose: Orchestrates ETL from Google Analytics API endpoints to BigQuery with robust data quality checks and audit logging.
Schedule: Runs at 6:00 AM and 6:00 PM every Wednesday.
Retries/Timeout: Each task retries up to 2 times with a 5-minute delay; tasks timeout after 3 minutes.

# Task Descriptions
1. extract_[endpoint]
    Extracts paginated data from the API endpoint.
    Uploads raw JSON files to Google Cloud Storage (GCS).
    Pushes records and source file paths to XCom for downstream tasks.

2. transform_[endpoint]
    Flattens and sanitizes the extracted records.
    Handles deduplication and conversion to Pandas DataFrame.
    Pushes cleaned DataFrame to XCom.

3. dq_check_[endpoint]
    Performs data quality checks:
        Required columns present
        Null value detection
        Duplicate keys
        Minimum record count
    Logs audit if failed.
    Short-circuits the pipeline if checks fail.

3. load_staging_[endpoint]
    Loads the cleaned DataFrame to a BigQuery staging table.

4. upsert_final_[endpoint]
    Upserts staged data into the final BigQuery target table using SQL MERGE.

5. audit_log_[endpoint]
    Logs audit information (success or failure) to BigQuery for governance.

## Monitoring & Governance (Bonus – Explanation)
# Data Quality Monitoring
# Metrics to Monitor:

    Required column presence
    Null values in key columns
    Duplicate key rows
    Record count anomalies
    Load success/failure rates

# Handling Data Quality Failures:

    Audit logs are written to a dedicated BigQuery audit table (load_audit) with status and error details.
    The pipeline short-circuits further processing for endpoints that fail data quality checks.

# Alerting Mechanisms:

    Airflow can be configured to send email or Slack alerts for task failures or data quality issues.
    Custom notifications can be triggered on failed audit log entries or by monitoring the audit table.

## Data Governance & Compliance
# Data Lineage Tracking:

    Use audit logs to capture run timestamps, source files, record counts, and statuses.
    Each audit entry provides lineage metadata (source, transformation time, destination).

# Metadata Capture:

    Source endpoint, extraction time, file paths, transformation steps, load timestamp, and record counts are all logged.
    Transformation logic is version-controlled in code and documented in the DAG.

# Data Privacy & PII Handling:

    Sensitive data should be masked, encrypted, or excluded at the extraction or transformation stage.
    Access to the pipeline and audit logs should be controlled via IAM.
    Consider using Google Cloud DLP for automated PII detection and redaction.

# Documentation & Cataloging:

    Maintain code documentation, DAG descriptions, and task docstrings.
    Use Data Catalog (Google Cloud or other) to register table schemas, data sources, and lineage.
    Document pipeline logic and governance policies in shared wikis or notebooks.
