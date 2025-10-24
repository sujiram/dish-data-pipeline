import json
import datetime
import requests
import pandas as pd
from pandas import json_normalize
from google.cloud import bigquery, storage
import config_file as cf

import argparse  
  
def main(run_type="full"):  
    endpoints = cf.ENDPOINTS  
    # ... rest of your code, replacing the hardcoded endpoints dict ...  
    for name, endpoint in endpoints.items():
        try:
            records, source_files = fetch_paginated_data(endpoint, name, bucket) 
            df = flatten_and_clean(name, records)
            valid, dq_issues = run_data_quality_checks(df, name)
            if not valid:
                if any("duplicate" in issue for issue in dq_issues):
                    print(f" Duplicates found for {name}, removing and continuing load.")
                    if name == "ga_sessions":
                        df = df.drop_duplicates(subset=["visitId", "source_file"])
                    elif name == "daily_visits":
                        df = df.drop_duplicates(subset=["visit_date", "source_file"])
                else:
                    log_audit(name, len(df), f"FAILED: Data Quality – {dq_issues}", source_files)
                    print(f" Skipping load for {name} due to data quality issues.")
                    continue
            load_to_staging(df, name)
            upsert_to_final(name)
            log_audit(name, len(df), "SUCCESS",source_files) 
        except Exception as e:
            print(f" Error processing {name}: {e}")
            try:
                log_audit(name, 0, f"FAILED: {str(e)}",source_files ) 
            except Exception as audit_e:
                print(f" Failed to write failure audit: {audit_e}")
        print("-" * 80)
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('--run_type', default='full', help='Type of run: full or test') 
    args = parser.parse_args() 
    main(run_type=args.run_type)

# Initialize clients explicitly with project from config
bq_client = bigquery.Client(project=cf.PROJECT_ID)
try:
    storage_client = storage.Client(project=cf.PROJECT_ID)
    bucket = storage_client.bucket(cf.BUCKET_NAME)
except Exception as e:
    print(" Could not connect to GCS.", e)
    bucket = None

# Helper: sanitize dataframe (only convert list/dict to JSON; leave datetimes)
def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    return df

# Fetch paginated API data (simplified version)
def fetch_paginated_data(endpoint, name, bucket):
    all_records = []
    blob_paths = []
    page = 1
    session = requests.Session()

    while True:
        url = f"{cf.BASE_URL}/{endpoint.lstrip('/')}" + f"?page={page}"
        try:
            resp = session.get(url, headers=cf.HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f" Error {resp.status_code} on page {page}: {resp.text}")
                break

            data = resp.json()
            records = data.get("records") or data.get("data") or []
            if not records:
                break
            all_records.extend(records)
            if bucket:
                today = datetime.datetime.now(datetime.timezone.utc)
                blob_path = (
                    f"raw_api_data/{name}/year={today.year}/month={today.month:02d}/"
                    f"day={today.day:02d}/{name}_page_{page}.json"
                )
                blob_paths.extend([blob_path])
                blob = bucket.blob(blob_path)
                blob.upload_from_string(json.dumps(records))

            if not data.get("pagination", {}).get("has_next") and not data.get("hasMore"):
                break

            page += 1

        except Exception as e:
            print(f" Error fetching page {page}: {e}")
            break

    print(f" Total records fetched for {name}: {len(all_records)}")
    return all_records,blob_paths


# Flatten and clean JSON records
def flatten_and_clean(name: str, records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    try:
        df = json_normalize(records, sep="_")
    except Exception:
        df = pd.DataFrame(records)

    # Add load_timestamp as timezone-aware UTC BEFORE any conversion that might change types
    df["load_timestamp"] = pd.Timestamp.now(tz="UTC")
    # If there is a visit_date column and it's string-like, leave for later conversion; add source_file
    df["source_file"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
    df = sanitize_dataframe(df)
    # Drop duplicates safely
    subset_cols = ["visit_date", "source_file"] if "visit_date" in df.columns else None
    df = df.drop_duplicates(subset=subset_cols)

    print(f" Cleaned {len(df)} rows for {name}")
    return df

# ✅ Data Quality Checks
def run_data_quality_checks(df: pd.DataFrame, table_name: str) -> tuple[bool, list[str]]:
    issues = []
    if df.empty:
        issues.append("Empty dataframe – no records to process.")
        return False, issues
    # Key column presence
    required_cols = {
        "daily_visits": ["visit_date", "total_visits"],
        "ga_sessions": ["visitId", "channelGrouping"]
    }.get(table_name, [])
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")
    # Null checks
    for col in required_cols:
        if col in df.columns and df[col].isnull().any():
            issues.append(f"Column '{col}' contains NULL values.")
    # Duplicate key checks
    if table_name == "daily_visits" and {"visit_date", "source_file"}.issubset(df.columns):
        dups = df.duplicated(subset=["visit_date", "source_file"]).sum()
        if dups > 0:
            issues.append(f"{dups} duplicate visit_date+source_file rows found.")
    elif table_name == "ga_sessions" and {"visitId", "source_file"}.issubset(df.columns):
        dups = df.duplicated(subset=["visitId", "source_file"]).sum()
        if dups > 0:
            issues.append(f"{dups} duplicate visitId+source_file rows found.")

    # Record count sanity (very low)
    if len(df) < 5:
        issues.append(f"Unusually low record count: {len(df)} rows.")
    is_valid = len(issues) == 0
    if not is_valid:
        print(f" Data quality issues for {table_name}: {issues}")
    return is_valid, issues
# Load dataframe into BigQuery staging — ensure types for visit_date and load_timestamp
def load_to_staging(df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"Skipped loading empty dataframe for {table_name}")
        return
    # Ensure visit_date is DATE-like (convert strings to date) if present
    if "visit_date" in df.columns:
        df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date
    if "visitId" in df.columns:
        df["visitId"] = df["visitId"].astype(str)
    if "load_timestamp" in df.columns:
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"], utc=True)

    df = sanitize_dataframe(df)

    table_id = f"{cf.PROJECT_ID}.{cf.DATASET}.staging_{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    print(f"⬆️ Loading {len(df)} rows into {table_id} ...")
    try:
        load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()
        print(f" Successfully loaded {len(df)} rows into {table_id}")
    except Exception as e:
        print(f" Error loading {table_name} to BigQuery: {e}")
        raise


def upsert_to_final(table_name):
    staging = f"{cf.DATASET}.staging_{table_name}"
    final = f"{cf.DATASET}.tgt_{table_name}"

    if table_name == "daily_visits":
        query = f"""
            MERGE `{cf.PROJECT_ID}.{final}` T
            USING `{cf.PROJECT_ID}.{staging}` S
            ON date(T.visit_date) = date(S.visit_date) AND T.source_file = S.source_file
            WHEN MATCHED THEN
              UPDATE SET
                T.total_visits = S.total_visits,
                T.load_timestamp = S.load_timestamp
            WHEN NOT MATCHED THEN
              INSERT (
                visit_date, total_visits, load_timestamp, source_file
              )
              VALUES (
                S.visit_date, S.total_visits, S.load_timestamp, S.source_file
              )
        """
    elif table_name == "ga_sessions":
        query = f"""
            MERGE `{cf.PROJECT_ID}.{final}` T
            USING (
                SELECT * EXCEPT(row_num) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY visitId, source_file
                        ORDER BY load_timestamp DESC
                    ) AS row_num
                    FROM `{cf.PROJECT_ID}.{staging}`
                )
                WHERE row_num = 1
            ) S
            ON T.visitId = S.visitId AND T.source_file = S.source_file
            WHEN MATCHED THEN
              UPDATE SET
                T.channelGrouping = S.channelGrouping,
                T.device_browser = S.device_browser,
                T.geoNetwork_country = S.geoNetwork_country,
                T.totals_hits = CAST(S.totals_hits AS STRING),
                T.load_timestamp = S.load_timestamp
            WHEN NOT MATCHED THEN
              INSERT (
                visitId, channelGrouping, device_browser,
                geoNetwork_country, totals_hits, load_timestamp, source_file
              )
              VALUES (
                S.visitId, S.channelGrouping, S.device_browser,
                S.geoNetwork_country, CAST(S.totals_hits AS STRING), S.load_timestamp, S.source_file
              )
        """
    else:
        print(f" No upsert logic defined for table: {table_name}")
        return

    try:
        bq_client.query(query).result()
        print(f" Upserted into final table: {final}")
    except Exception as e:
        print(f" Merge failed for {table_name}: {e}")
        raise

# Audit logging
def log_audit(table_name, record_count, status, source_files):
    audit_table = f"{cf.PROJECT_ID}.{cf.DATASET}.load_audit"
    # Ensure source_files is a flat list of strings
    if isinstance(source_files, str):
        source_files = [source_files]

    rows = [{
        "table_name": table_name,
        "record_count": record_count,
        "status": status,
        "load_timestamp": pd.Timestamp.now(tz="UTC"),
        "source_files": source_files
    }]
    audit_df = pd.DataFrame(rows)
    audit_df["source_files"] = audit_df["source_files"].astype("object")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    try:
        bq_client.load_table_from_dataframe(audit_df, audit_table, job_config=job_config).result()
        print(f" Audit logged for {table_name}")
    except Exception as e:
        print(f" Failed to log audit for {table_name}: {e}")