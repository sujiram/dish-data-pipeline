# 📊 DISH Data Engineer Technical Assessment

## 🧾 Expected Deliverables Overview

| Deliverable | Description |
|--------------|-------------|
| **`data_pipeline.py`** | Main extraction, transformation, and loading (ETL) script. It fetches paginated API data, performs cleaning, applies data quality checks, and loads into BigQuery. |
| **`requirements.txt`** | List of Python dependencies required to execute the pipeline locally or via Docker/Airflow. |
| **`README.md`** | Project documentation containing setup, execution, and validation instructions. |
| **Sample Output Files** | Example API extraction results saved in GCS (`raw_api_data/...`) and local compressed file (`sample_output.zip`). |
| **Populated BigQuery Tables / CSVs** | Final target tables: `tgt_daily_visits`, `tgt_ga_sessions`, and audit log `load_audit`. Equivalent CSVs provided if BigQuery access is restricted. |

---

## 🚀 Project Overview

This project demonstrates an end-to-end data engineering solution built for **DISH Deutschland’s Data Engineer Technical Assessment**.

The pipeline:
- Extracts data from **two API endpoints** (`daily-visits`, `ga-sessions-data`).
- Saves raw API responses into **Google Cloud Storage (GCS)** partitioned by date.
- Cleans and flattens JSON data.
- Runs **data quality (DQ) checks** to validate schema, nulls, duplicates, and record counts.
- Loads data into **BigQuery staging tables**.
- Uses **MERGE statements** to upsert into final `tgt_` tables.
- Logs all job statuses and file references into an **audit table**.
- Supports orchestration using **Apache Airflow** and deployment via **Docker**.

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository
```bash
git clone https://github.com/<your-username>/dish-data-pipeline.git
cd dish-data-pipeline
```

### 2️⃣ Create Virtual Environment & Install Dependencies
```bash
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3️⃣ Configure Environment
Copy the configuration template and update it with your details:
```bash
cp pipeline/config_file_template.py pipeline/config_file.py
```

Edit `config_file.py` with your actual project values:
```python
PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "your-gcs-bucket"
DATASET = "dish_dataset"
BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
HEADERS = {"Content-Type": "application/json"}
ENDPOINTS = {
    "daily_visits": "daily-visits",
    "ga_sessions": "ga-sessions-data"
}
```

---

## ▶️ Execution Options

### 🧩 Option 1 — Run Locally
```bash
python pipeline/data_pipeline.py
```

### 🐳 Option 2 — Run with Docker
```bash
docker build -t dish-data-pipeline .
docker run --rm   -v $(pwd)/pipeline/config_file.py:/app/pipeline/config_file.py   dish-data-pipeline
```

### ☁️ Option 3 — Airflow Orchestration
Use the DAG file `dags/etl_google_analytics_dag.py`.

**Schedule:** Runs at 6:00 AM and 6:00 PM on **Wednesdays**  
**Retries:** 2 attempts  
**Delay:** 5 minutes between retries  
**Timeout:** 3 minutes per task  

---

## 🧪 Data Flow Summary

### 📥 Extract
- API calls to:
  - `/daily-visits`
  - `/ga-sessions-data`
- Each page of API data is stored in:
  ```
  gs://<your-bucket>/raw_api_data/<endpoint>/year=YYYY/month=MM/day=DD/
  ```

### 🔧 Transform
- Nested JSON flattened using `pandas.json_normalize()`
- Added metadata:
  - `load_timestamp` (UTC)
  - `source_file`
- Data quality checks:
  - Missing column validation
  - Null detection
  - Duplicate detection
  - Low record count alerts
- Deduplication:
  - `daily_visits`: `(visit_date, source_file)`
  - `ga_sessions`: `(visitId, source_file)`

### 📤 Load
- Data loaded to **BigQuery staging tables**:
  ```
  dish_dataset.staging_daily_visits
  dish_dataset.staging_ga_sessions
  ```
- Incrementally merged to final **target tables**:
  ```
  dish_dataset.tgt_daily_visits
  dish_dataset.tgt_ga_sessions
  ```
- Load audit logged in:
  ```
  dish_dataset.load_audit
  ```

---

## 🧮 Sample Output (Included in `sample_output.zip`)

```
sample_output/
├── raw_api_data/
│   ├── daily_visits/
│   └── ga_sessions/
├── tgt_daily_visits.csv
├── tgt_ga_sessions.csv
└── load_audit.csv
```

### Example — `load_audit.csv`
| table_name     | record_count | status   | load_timestamp       | source_files |
|----------------|---------------|----------|----------------------|---------------|
| daily_visits   | 367           | SUCCESS  | 2025-10-22 18:30:00Z | raw_api_data/daily_visits/... |
| ga_sessions    | 2509          | SUCCESS  | 2025-10-22 18:32:00Z | raw_api_data/ga_sessions/... |

---

## 🧠 Data Quality & Governance

### Data Quality Monitoring
| Check Type | Description | Action |
|-------------|-------------|--------|
| Missing Columns | Verifies expected schema | Skips or logs as failure |
| Nulls in Key Fields | Detects missing keys | Fails and logs |
| Duplicates | Deduplicates on primary keys | Removes duplicates and continues |
| Record Count | Alerts on low volume (<5 rows) | Logs warning |

### Data Governance
- **Lineage:** API → GCS → BigQuery (staging → target)
- **Metadata:** `load_timestamp`, `source_file`, `record_count`, `status`
- **Audit Trail:** Each run logged in `load_audit`
- **Data Privacy:** No PII stored; config file excluded from GitHub
- **Documentation:** DAG details in `dags/DAG.md`

---

## 🧩 Folder Structure

```
dish-data-pipeline/
│
├── pipeline/
│   ├── data_pipeline.py
│   ├── config_file_template.py
│   └── config_file.py           # (local only; excluded from GitHub)
│
├── dags/
│   ├── etl_google_analytics_dag.py
│   └── DAG.md
│
├── docker/
│   └── Dockerfile
│
├── sample_output.zip
├── requirements.txt
├── setup.py
├── .gitignore
└── README.md
```

---

## 📊 BigQuery Tables Overview

| Layer | Table | Purpose |
|-------|--------|----------|
| **Raw (GCS)** | raw_api_data/... | Raw JSON from APIs |
| **Staging** | `staging_daily_visits`, `staging_ga_sessions` | Temporary clean data |
| **Target** | `tgt_daily_visits`, `tgt_ga_sessions` | Final merged production tables |
| **Audit** | `load_audit` | Tracks load metadata and DQ status |

---

## 📘 Contact

**Author:** Sumathi R  
**Role:** Data Engineer Candidate  
**Location:** Germany  
**Tools Used:** Python · BigQuery · GCS · Airflow · Docker  
