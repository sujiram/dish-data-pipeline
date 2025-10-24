# pipeline/config_file_template.py

PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "your-gcs-bucket"
DATASET = "dish_dataset"
BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
HEADERS = {"Content-Type": "application/json"}

ENDPOINTS = {
    "daily_visits": "daily-visits",
    "ga_sessions": "ga-sessions-data"
}

DATASET = "dish_dataset"